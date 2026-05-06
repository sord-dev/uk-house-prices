import os
import sys
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import BackgroundTasks, FastAPI, HTTPException

sys.path.insert(0, "/app")
from ingest import LandRegistryIngestor

jobs: dict = {}
_summary_cache: Dict[str, dict] = {}

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'dbname': os.getenv('POSTGRES_DB', 'house_prices'),
    'user': os.getenv('POSTGRES_USER', 'prices'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

# Ollama configuration
OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'qwen2.5:1.5b')

app = FastAPI(root_path="/api", title="UK House Prices API")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/ingest/monthly", status_code=202)
async def ingest_monthly(background_tasks: BackgroundTasks):
    job_id = "monthly"
    if jobs.get(job_id, {}).get("status") == "running":
        raise HTTPException(409, "Monthly ingest already running")

    async def _job():
        jobs[job_id] = {"status": "running"}
        try:
            await LandRegistryIngestor().ingest_monthly_updates()
            jobs[job_id] = {"status": "complete"}
            _summary_cache.clear()
        except Exception as e:
            jobs[job_id] = {"status": "failed", "error": str(e)}

    background_tasks.add_task(_job)
    return {"job_id": job_id, "status": "accepted"}


@app.post("/ingest/yearly", status_code=202)
async def ingest_yearly(year: int, background_tasks: BackgroundTasks):
    job_id = f"yearly_{year}"
    if jobs.get(job_id, {}).get("status") == "running":
        raise HTTPException(409, f"Yearly ingest for {year} already running")

    async def _job():
        jobs[job_id] = {"status": "running"}
        try:
            await LandRegistryIngestor().ingest_yearly_data(year)
            jobs[job_id] = {"status": "complete"}
            _summary_cache.clear()
        except Exception as e:
            jobs[job_id] = {"status": "failed", "error": str(e)}

    background_tasks.add_task(_job)
    return {"job_id": job_id, "status": "accepted"}


@app.post("/ingest/backfill", status_code=202)
async def ingest_backfill(background_tasks: BackgroundTasks):
    job_id = "backfill"
    if jobs.get(job_id, {}).get("status") == "running":
        raise HTTPException(409, "Backfill already running")

    async def _job():
        jobs[job_id] = {"status": "running"}
        try:
            await LandRegistryIngestor().ingest_backfill()
            jobs[job_id] = {"status": "complete"}
            _summary_cache.clear()
        except Exception as e:
            jobs[job_id] = {"status": "failed", "error": str(e)}

    background_tasks.add_task(_job)
    return {"job_id": job_id, "status": "accepted"}


@app.get("/ingest/coverage")
async def ingest_coverage():
    conn = await psycopg.AsyncConnection.connect(**DB_CONFIG)
    await conn.set_autocommit(True)
    try:
        ingestor = LandRegistryIngestor()
        covered = await ingestor.get_covered_months(conn)
        missing = await ingestor.detect_missing_months(conn)
        return {
            "covered_months": [m.isoformat() for m in covered],
            "missing_months": [m.isoformat() for m in missing],
        }
    except Exception as e:
        raise HTTPException(500, f"Coverage check failed: {e}")
    finally:
        await conn.close()


@app.get("/ingest/{job_id}/status")
async def job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]


async def get_monthly_summary_data():
    """Query PostgreSQL for recent monthly house price data."""
    try:
        conn = await psycopg.AsyncConnection.connect(**DB_CONFIG)
        await conn.set_autocommit(True)
        
        # Build filter for both counties and London boroughs if TARGET_COUNTIES is specified
        target_counties = os.getenv('TARGET_COUNTIES')
        area_filter = ""
        if target_counties:
            areas = [f"'{area.strip().upper()}'" for area in target_counties.split(',')]
            area_filter = f"AND UPPER(area_name) IN ({','.join(areas)})"
        
        query = f"""
        WITH area_data AS (
            -- County-level data
            SELECT
                county as area_name,
                'County' as area_type,
                COUNT(*) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                    AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as transactions,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as current_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as prev_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '13 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '12 months') as same_month_last_year_median,
                to_char(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month', 'FMMonth YYYY') as reporting_month
            FROM transactions
            WHERE ppd_type = 'A'
              AND record_status = 'A'
              AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '13 months'
              AND UPPER(county) != 'GREATER LONDON'
            GROUP BY county

            UNION ALL

            -- London borough data
            SELECT
                district as area_name,
                'London Borough' as area_type,
                COUNT(*) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                    AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as transactions,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as current_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as prev_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '13 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '12 months') as same_month_last_year_median,
                to_char(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month', 'FMMonth YYYY') as reporting_month
            FROM transactions
            WHERE ppd_type = 'A'
              AND record_status = 'A'
              AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '13 months'
              AND UPPER(county) = 'GREATER LONDON'
              AND district IS NOT NULL
            GROUP BY district
        ),
        with_changes AS (
            SELECT *,
                ROUND((100.0 * (prev_month_median - same_month_last_year_median)
                    / NULLIF(same_month_last_year_median, 0))::NUMERIC, 1) as yoy_change_pct,
                ROUND((100.0 * (current_month_median - prev_month_median)
                    / NULLIF(prev_month_median, 0))::NUMERIC, 1) as mom_change_pct
            FROM area_data
        )
        SELECT area_name, area_type, transactions, current_month_median, prev_month_median,
               same_month_last_year_median, mom_change_pct, yoy_change_pct, reporting_month
        FROM with_changes
        WHERE transactions > 10
          {area_filter}
        ORDER BY transactions DESC;
        """
        
        date_range_query = """
        SELECT
            to_char(MIN(date), 'YYYY-MM-DD') as min_date,
            to_char(MAX(date), 'YYYY-MM-DD') as max_date
        FROM transactions
        WHERE ppd_type = 'A'
          AND record_status = 'A'
          AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
          AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
        """

        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query)
            results = await cursor.fetchall()
            await cursor.execute(date_range_query)
            dr = await cursor.fetchone()

        await conn.close()
        date_range = {"from": dr['min_date'], "to": dr['max_date']} if dr and dr['min_date'] else None
        return [dict(row) for row in results], date_range

    except Exception as e:
        raise HTTPException(500, f"Database query failed: {e}")


def _format_row(row: Dict) -> str:
    area = (row['area_name'] or 'Unknown').title()
    area_type = row['area_type'] or 'Area'
    tx = int(row['transactions'])
    median = row['current_month_median']
    median_str = f"median £{int(median):,}" if median else "median N/A"
    mom = row['mom_change_pct']
    mom_str = f"{float(mom):+.1f}% MoM" if mom is not None else ""
    parts = [f"{area} ({area_type}): {tx:,} tx, {median_str}"]
    if mom_str:
        parts[0] += f", {mom_str}"
    return parts[0]


def _select_notable(data: List[Dict]) -> Dict:
    reporting_month = data[0].get('reporting_month') or 'Unknown'
    total_tx = sum(int(r['transactions']) for r in data)
    with_mom = [r for r in data if r['mom_change_pct'] is not None and r['current_month_median']]
    with_yoy = [r for r in data if r['yoy_change_pct'] is not None and r['same_month_last_year_median']]
    top_by_volume = data if len(data) <= 15 else data[:10]
    top_gainers = sorted(with_mom, key=lambda r: float(r['mom_change_pct']), reverse=True)[:3]
    top_fallers = sorted(with_mom, key=lambda r: float(r['mom_change_pct']))[:3]
    top_yoy_gainers = sorted(with_yoy, key=lambda r: float(r['yoy_change_pct']), reverse=True)[:3]
    top_yoy_fallers = sorted([r for r in with_yoy if float(r['yoy_change_pct']) < 0], key=lambda r: float(r['yoy_change_pct']))[:3]
    return {
        "reporting_month": reporting_month,
        "total_tx": total_tx,
        "with_mom": with_mom,
        "with_yoy": with_yoy,
        "top_by_volume": top_by_volume,
        "top_gainers": top_gainers,
        "top_fallers": top_fallers,
        "top_yoy_gainers": top_yoy_gainers,
        "top_yoy_fallers": top_yoy_fallers,
    }


def _serialise_area(row: Dict) -> Dict:
    return {
        "area_name": (row['area_name'] or 'Unknown').title(),
        "area_type": row['area_type'] or 'Area',
        "transactions": int(row['transactions']),
        "current_month_median": int(row['current_month_median']) if row['current_month_median'] else None,
        "prev_month_median": int(row['prev_month_median']) if row['prev_month_median'] else None,
        "same_month_last_year_median": int(row['same_month_last_year_median']) if row['same_month_last_year_median'] else None,
        "mom_change_pct": float(row['mom_change_pct']) if row['mom_change_pct'] is not None else None,
        "yoy_change_pct": float(row['yoy_change_pct']) if row['yoy_change_pct'] is not None else None,
    }


def _format_row_yoy(row: Dict) -> str:
    area = (row['area_name'] or 'Unknown').title()
    area_type = row['area_type'] or 'Area'
    yoy = float(row['yoy_change_pct'])
    prev_med = row['prev_month_median']
    prev_str = f", median £{int(prev_med):,}" if prev_med else ""
    return f"{area} ({area_type}){prev_str}, {yoy:+.1f}% YoY"


async def generate_ai_summary(notable: Dict) -> str:
    """Send pre-selected notable data to Ollama for AI summary generation."""
    reporting_month = notable['reporting_month']
    total_tx = notable['total_tx']
    with_mom = notable['with_mom']
    with_yoy = notable['with_yoy']
    top_by_volume = notable['top_by_volume']
    top_gainers = notable['top_gainers']
    top_fallers = notable['top_fallers']
    top_yoy_gainers = notable['top_yoy_gainers']
    top_yoy_fallers = notable['top_yoy_fallers']

    def section(rows: List[Dict]) -> str:
        return "\n".join(f"  {_format_row(r)}" for r in rows)

    def section_yoy(rows: List[Dict]) -> str:
        return "\n".join(f"  {_format_row_yoy(r)}" for r in rows)

    movers_block = ""
    if with_mom:
        movers_block = f"""
Biggest price increases (MoM):
{section(top_gainers)}

Biggest price falls (MoM):
{section(top_fallers)}"""
    else:
        movers_block = "\nNote: Month-over-month price comparison data is not yet available for this reporting period — do not comment on MoM price direction."

    yoy_block = ""
    if with_yoy:
        yoy_block = f"""
Biggest year-on-year increases:
{section_yoy(top_yoy_gainers)}

Biggest year-on-year falls:
{section_yoy(top_yoy_fallers)}"""
    else:
        yoy_block = "\nNote: Year-on-year comparison data is not available — do not comment on annual price direction."

    has_movers = bool(with_mom or with_yoy)
    sentence_3_instruction = (
        "Highlight the single most notable price mover (MoM or YoY) with its percentage."
        if has_movers else
        "State that price comparison data is not yet available for this period."
    )

    prompt = f"""You are a UK property market analyst. Write a push notification briefing of exactly 3 sentences.

Rules:
- Use ONLY the numbers provided. Do not invent any figures.
- If a median is N/A, skip price for that area entirely — do not say "not available".
- Do not include labels like "Sentence 1" in your output — just write the sentences.

What each sentence must cover:
1. The reporting period, total transaction count, and number of areas monitored.
2. The top 2-3 areas by transaction volume, with counts and medians where available.
3. {sentence_3_instruction}

Data:
Reporting period: {reporting_month}
Total transactions: {total_tx:,}
Areas monitored: {len(top_by_volume)}

Markets by volume:
{section(top_by_volume)}
{movers_block}
{yoy_block}"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1},
    }
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(f"{OLLAMA_HOST}/api/generate", json=payload)
            response.raise_for_status()
            
            result = response.json()
            if 'response' not in result:
                raise HTTPException(503, "Invalid response format from AI service")
                
            return result['response'].strip()
            
    except httpx.TimeoutException:
        raise HTTPException(503, "AI service timeout - please try again")
    except httpx.HTTPStatusError as e:
        raise HTTPException(503, f"AI service error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(503, f"AI service unavailable: {e}")


@app.post("/summarise/monthly")
async def summarise_monthly():
    """Generate AI summary of recent monthly house price data."""
    try:
        data, date_range = await get_monthly_summary_data()
        if not data:
            raise HTTPException(503, "No data available for summary")
        notable = _select_notable(data)
        cache_key = notable["reporting_month"]
        if cache_key in _summary_cache:
            return _summary_cache[cache_key]
        summary = await generate_ai_summary(notable)
        result = {
            "summary": summary,
            "data_period": notable["reporting_month"],
            "actual_date_range": date_range,
            "areas_analysed": len(data),
            "data": {
                "top_by_volume": [_serialise_area(r) for r in notable["top_by_volume"]],
                "top_gainers": [_serialise_area(r) for r in notable["top_gainers"]],
                "top_fallers": [_serialise_area(r) for r in notable["top_fallers"]],
                "top_yoy_gainers": [_serialise_area(r) for r in notable["top_yoy_gainers"]],
                "top_yoy_fallers": [_serialise_area(r) for r in notable["top_yoy_fallers"]],
            },
        }
        _summary_cache[cache_key] = result
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Summary generation failed: {e}")
