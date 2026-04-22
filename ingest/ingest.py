#!/usr/bin/env python3
"""
UK House Prices Data Ingest Service

Downloads and processes HM Land Registry Price Paid Data into PostgreSQL.
Handles full baseline import, yearly backfills, and monthly delta updates.

Usage:
    python ingest.py --mode full
    python ingest.py --mode yearly --year 2023
    python ingest.py --mode monthly
    python ingest.py --mode postcode-lookup
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import click
import httpx
import psycopg
import structlog
from psycopg import sql
from psycopg.rows import dict_row
from tqdm import tqdm
from dotenv import load_dotenv

from parsing import parse_csv_line, parse_transaction_row, should_include_transaction, ParseResult

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Land Registry data URLs
LAND_REGISTRY_URLS = {
    'full': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-complete.csv',
    'yearly': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-{year}.csv',
    'monthly': 'https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv'
}

ONS_POSTCODE_URL = 'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-latest-centroids.csv'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'dbname': os.getenv('POSTGRES_DB', 'house_prices'),
    'user': os.getenv('POSTGRES_USER', 'prices'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

class IngestionError(Exception):
    """Base exception for ingestion errors."""
    pass

class DatabaseError(IngestionError):
    """Database operation errors."""
    pass

class DownloadError(IngestionError):
    """Data download errors."""
    pass

class ValidationError(IngestionError):
    """Data validation errors."""
    pass

class LandRegistryIngestor:
    """Main class for ingesting Land Registry data."""
    
    def __init__(self):
        self.db_config = DB_CONFIG
        self.session_stats = {
            'downloaded_rows': 0,
            'processed_rows': 0,
            'inserted_rows': 0,
            'updated_rows': 0,
            'deleted_rows': 0,
            'filtered_rows': 0,
            'error_rows': 0
        }
        
    async def get_database_connection(self) -> psycopg.AsyncConnection:
        """Get async database connection."""
        try:
            conn = await psycopg.AsyncConnection.connect(**self.db_config)
            await conn.set_autocommit(True)
            return conn
        except Exception as e:
            logger.error("Failed to connect to database", error=str(e), config=self.db_config)
            raise DatabaseError(f"Database connection failed: {e}")
    
    async def stream_csv_data(self, conn: psycopg.AsyncConnection, url: str, batch_size: int = 1000, bulk_mode: bool = False):
        """Stream CSV data directly from HTTP response and process in chunks."""
        logger.info("Starting streaming download and processing", url=url, batch_size=batch_size, bulk_mode=bulk_mode)
        
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                async with client.stream('GET', url, follow_redirects=True) as response:
                    response.raise_for_status()
                    
                    content_length = response.headers.get('content-length')
                    total_mb = round(int(content_length) / 1024 / 1024, 2) if content_length else None
                    logger.info("Starting stream processing", url=url, size_mb=total_mb, mode="COPY bulk" if bulk_mode else "upsert delta")
                    
                    # Initialize streaming CSV processor
                    await self._process_streaming_csv(conn, response, batch_size, total_mb, bulk_mode)
                    
        except httpx.RequestError as e:
            logger.error("Download failed", url=url, error=str(e))
            raise DownloadError(f"Failed to download {url}: {e}")
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error during download", url=url, status=e.response.status_code)
            raise DownloadError(f"HTTP {e.response.status_code} for {url}")
    
    async def _process_streaming_csv(self, conn: psycopg.AsyncConnection, response, batch_size: int, total_mb: Optional[float], bulk_mode: bool = False):
        """Process CSV data as it streams from HTTP response."""
        buffer = ""
        batch = []
        bytes_processed = 0
        
        with tqdm(desc="Processing transactions", 
                 unit="rows", 
                 postfix={'MB processed': 0, 'inserted': 0, 'filtered': 0}) as pbar:
            
            async for chunk in response.aiter_bytes(chunk_size=65536):  # 64KB chunks
                bytes_processed += len(chunk)
                buffer += chunk.decode('utf-8', errors='replace')
                
                # Process complete lines from buffer
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if not line:  # Skip empty lines
                        continue
                        
                    self.session_stats['downloaded_rows'] += 1
                    pbar.update(1)
                    
                    # Parse CSV row
                    try:
                        # Handle quoted fields in CSV
                        row = parse_csv_line(line)
                        result, transaction, error_msg = parse_transaction_row(row)
                        
                        if result != ParseResult.SUCCESS:
                            if error_msg:
                                logger.warning("Failed to parse CSV line", error=error_msg, line=line[:100])
                            self.session_stats['error_rows'] += 1
                            continue
                            
                        # Transaction is guaranteed to be valid here
                        assert transaction is not None, "Transaction should not be None when parsing succeeds"
                        if not should_include_transaction(transaction):
                            self.session_stats['filtered_rows'] += 1
                            continue
                            
                        batch.append(transaction)
                        self.session_stats['processed_rows'] += 1
                        
                        # Process batch when it reaches batch_size
                        if len(batch) >= batch_size:
                            if bulk_mode:
                                await self.process_batch_bulk(conn, batch)
                            else:
                                await self.process_batch(conn, batch)
                            batch = []
                            
                            # Update progress bar
                            mb_processed = round(bytes_processed / 1024 / 1024, 1)
                            pbar.set_postfix({
                                'MB processed': mb_processed,
                                'inserted': self.session_stats['inserted_rows'],
                                'updated': self.session_stats['updated_rows'],
                                'filtered': self.session_stats['filtered_rows']
                            })
                            
                    except Exception as e:
                        logger.warning("Failed to process CSV line", error=str(e), line=line[:100])
                        self.session_stats['error_rows'] += 1
            
            # Process any remaining lines in buffer
            if buffer.strip():
                try:
                    row = parse_csv_line(buffer.strip())
                    result, transaction, error_msg = parse_transaction_row(row)
                    
                    if result == ParseResult.SUCCESS:
                        assert transaction is not None, "Transaction should not be None when parsing succeeds"
                        if should_include_transaction(transaction):
                            batch.append(transaction)
                            self.session_stats['processed_rows'] += 1
                        else:
                            self.session_stats['filtered_rows'] += 1
                    else:
                        if error_msg:
                            logger.warning("Failed to process final buffer", error=error_msg)
                        self.session_stats['error_rows'] += 1
                        
                except Exception as e:
                    logger.warning("Failed to process final buffer", error=str(e))
                    self.session_stats['error_rows'] += 1
            
            # Process final batch
            if batch:
                if bulk_mode:
                    await self.process_batch_bulk(conn, batch)
                else:
                    await self.process_batch(conn, batch)
        
        logger.info("Streaming processing complete", 
                   bytes_processed=bytes_processed,
                   mb_processed=round(bytes_processed / 1024 / 1024, 2),
                   stats=self.session_stats)
    
    async def upsert_transaction(self, conn: psycopg.AsyncConnection, transaction: Dict):
        """Insert or update a transaction record."""
        async with conn.cursor() as cursor:
            if transaction['record_status'] == 'D':
                # Handle delete
                await cursor.execute(
                    "DELETE FROM transactions WHERE transaction_id = %s",
                    (transaction['transaction_id'],)
                )
                self.session_stats['deleted_rows'] += 1
            else:
                # Handle insert/update
                insert_sql = """
                INSERT INTO transactions (
                    transaction_id, price, date, postcode, property_type,
                    new_build, tenure, paon, saon, street, locality,
                    town, district, county, ppd_type, record_status
                ) VALUES (
                    %(transaction_id)s, %(price)s, %(date)s, %(postcode)s, %(property_type)s,
                    %(new_build)s, %(tenure)s, %(paon)s, %(saon)s, %(street)s, %(locality)s,
                    %(town)s, %(district)s, %(county)s, %(ppd_type)s, %(record_status)s
                )
                ON CONFLICT (transaction_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    date = EXCLUDED.date,
                    postcode = EXCLUDED.postcode,
                    property_type = EXCLUDED.property_type,
                    new_build = EXCLUDED.new_build,
                    tenure = EXCLUDED.tenure,
                    paon = EXCLUDED.paon,
                    saon = EXCLUDED.saon,
                    street = EXCLUDED.street,
                    locality = EXCLUDED.locality,
                    town = EXCLUDED.town,
                    district = EXCLUDED.district,
                    county = EXCLUDED.county,
                    ppd_type = EXCLUDED.ppd_type,
                    record_status = EXCLUDED.record_status,
                    ingested_at = now()
                """
                
                await cursor.execute(insert_sql, transaction)
                
                if cursor.rowcount == 1:
                    self.session_stats['inserted_rows'] += 1
                else:
                    self.session_stats['updated_rows'] += 1
    

    
    async def process_batch_bulk(self, conn: psycopg.AsyncConnection, batch: List[Dict]):
        """Process batch using PostgreSQL COPY for maximum performance (bulk loads only)."""
        if not batch:
            return
            
        try:
            async with conn.cursor() as cursor:
                # Use psycopg3's copy context manager for efficient bulk insert
                async with cursor.copy("COPY transactions (transaction_id, price, date, postcode, property_type, new_build, tenure, paon, saon, street, locality, town, district, county, ppd_type, record_status) FROM STDIN") as copy:
                    for transaction in batch:
                        # Prepare row data in correct order
                        row_data = (
                            transaction['transaction_id'],
                            transaction['price'], 
                            transaction['date'],
                            transaction['postcode'],
                            transaction['property_type'],
                            transaction['new_build'],
                            transaction['tenure'],
                            transaction['paon'],
                            transaction['saon'],
                            transaction['street'],
                            transaction['locality'],
                            transaction['town'],
                            transaction['district'],
                            transaction['county'],
                            transaction['ppd_type'],
                            transaction['record_status']
                        )
                        
                        await copy.write_row(row_data)
                
            self.session_stats['inserted_rows'] += len(batch)
            logger.debug("Bulk inserted batch", count=len(batch))
            
        except Exception as e:
            logger.error("Failed to bulk insert batch", 
                        batch_size=len(batch),
                        error=str(e))
            # Fallback to individual processing if COPY fails
            logger.info("Falling back to individual upserts for failed batch")
            await self.process_batch(conn, batch)
    
    async def process_batch(self, conn: psycopg.AsyncConnection, batch: List[Dict]):
        """Process a batch of transactions with individual upserts (for monthly deltas with A/C/D handling)."""
        for transaction in batch:
            try:
                await self.upsert_transaction(conn, transaction)
            except Exception as e:
                logger.error("Failed to upsert transaction", 
                           transaction_id=transaction.get('transaction_id'),
                           error=str(e))
                self.session_stats['error_rows'] += 1
    
    async def refresh_materialized_views(self, conn: psycopg.AsyncConnection):
        """Refresh materialized views after data ingestion."""
        logger.info("Refreshing materialized views")
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT refresh_monthly_stats()")
        logger.info("Materialized views refreshed")
    
    async def get_data_freshness(self, conn: psycopg.AsyncConnection) -> Dict:
        """Get data freshness statistics."""
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute("SELECT * FROM get_data_freshness()")
            result = await cursor.fetchone()
            return dict(result) if result else {}
    
    async def ingest_full_dataset(self):
        """Ingest the complete historical dataset using bulk COPY for maximum performance."""
        logger.info("Starting full dataset ingestion with bulk COPY")
        
        conn = await self.get_database_connection()
        try:
            await self.stream_csv_data(conn, LAND_REGISTRY_URLS['full'], batch_size=5000, bulk_mode=True)
            await self.refresh_materialized_views(conn)
            
            freshness = await self.get_data_freshness(conn)
            logger.info("Full ingestion complete", stats=self.session_stats, freshness=freshness)
            
        finally:
            await conn.close()
    
    async def check_year_has_data(self, conn: psycopg.AsyncConnection, year: int) -> bool:
        """Check if database already contains data for the specified year."""
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM transactions WHERE EXTRACT(YEAR FROM date) = %s LIMIT 1",
                (year,)
            )
            result = await cursor.fetchone()
            return result[0] > 0 if result else False
    
    async def ingest_yearly_data(self, year: int, force_bulk: bool = False):
        """Ingest data for a specific year, intelligently choosing bulk vs upsert mode."""
        conn = await self.get_database_connection()
        
        try:
            # Check if data already exists for this year
            has_existing_data = await self.check_year_has_data(conn, year)
            
            if has_existing_data and not force_bulk:
                logger.info("Found existing data for year, using upsert mode to avoid COPY conflicts", year=year)
                bulk_mode = False
                batch_size = 1000
                mode_desc = "upsert mode (existing data detected)"
            else:
                if force_bulk and has_existing_data:
                    logger.warning("Force bulk mode enabled despite existing data - COPY may fail on conflicts", year=year)
                logger.info("Using bulk COPY mode for maximum performance", year=year)
                bulk_mode = True
                batch_size = 5000
                mode_desc = "bulk COPY mode"
                
            logger.info("Starting yearly data ingestion", year=year, mode=mode_desc)
            
            url = LAND_REGISTRY_URLS['yearly'].format(year=year)
            await self.stream_csv_data(conn, url, batch_size=batch_size, bulk_mode=bulk_mode)
            await self.refresh_materialized_views(conn)
            
            freshness = await self.get_data_freshness(conn)
            logger.info("Yearly ingestion complete", year=year, mode=mode_desc, stats=self.session_stats, freshness=freshness)
            
        finally:
            await conn.close()
    
    async def ingest_monthly_updates(self):
        """Ingest monthly delta updates using individual upserts for A/C/D record handling."""
        logger.info("Starting monthly updates ingestion with individual upserts for A/C/D handling")
        
        conn = await self.get_database_connection()
        try:
            # Use smaller batches and individual upserts for monthly deltas
            await self.stream_csv_data(conn, LAND_REGISTRY_URLS['monthly'], batch_size=500, bulk_mode=False)
            await self.refresh_materialized_views(conn)
            
            freshness = await self.get_data_freshness(conn)
            logger.info("Monthly updates complete", stats=self.session_stats, freshness=freshness)
            
        finally:
            await conn.close()

@click.command()
@click.option('--mode', 
              type=click.Choice(['full', 'yearly', 'monthly', 'postcode-lookup']), 
              required=True,
              help='Ingestion mode')
@click.option('--year', 
              type=int, 
              help='Year for yearly mode (e.g., 2023)')
@click.option('--force-bulk', 
              is_flag=True,
              help='Force bulk COPY mode for yearly ingests even if data exists (may fail on conflicts)')
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              default='INFO',
              help='Logging level')
def main(mode: str, year: Optional[int], force_bulk: bool, log_level: str):
    """UK House Prices Data Ingestion Service."""
    
    # Configure logging
    logging.basicConfig(level=getattr(logging, log_level))
    
    # Log startup information
    logger.info("Starting ingestion service", 
                mode=mode, 
                year=year,
                target_counties=os.getenv('TARGET_COUNTIES', 'ESSEX,HERTFORDSHIRE,KENT,SURREY,CAMBRIDGESHIRE').split(','),
                db_host=DB_CONFIG['host'])
    
    ingestor = LandRegistryIngestor()
    
    try:
        if mode == 'full':
            asyncio.run(ingestor.ingest_full_dataset())
        elif mode == 'yearly':
            if not year:
                click.echo("Error: --year is required for yearly mode", err=True)
                sys.exit(1)
            asyncio.run(ingestor.ingest_yearly_data(year, force_bulk=force_bulk))
        elif mode == 'monthly':
            asyncio.run(ingestor.ingest_monthly_updates())
        elif mode == 'postcode-lookup':
            click.echo("Postcode lookup ingestion not yet implemented", err=True)
            sys.exit(1)
            
    except Exception as e:
        logger.error("Ingestion failed", error=str(e))
        sys.exit(1)

if __name__ == '__main__':
    main()