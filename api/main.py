import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse

sys.path.insert(0, "/app")
from ingest import LandRegistryIngestor

jobs: dict = {}

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


def run_job(job_id: str, coro):
    async def _run():
        jobs[job_id] = {"status": "running"}
        try:
            await coro
            jobs[job_id] = {"status": "complete"}
        except Exception as e:
            jobs[job_id] = {"status": "failed", "error": str(e)}

    asyncio.create_task(_run())


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
        except Exception as e:
            jobs[job_id] = {"status": "failed", "error": str(e)}

    background_tasks.add_task(_job)
    return {"job_id": job_id, "status": "accepted"}


@app.get("/ingest/{job_id}/status")
async def job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]


async def get_monthly_summary_data() -> List[Dict]:
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
                COUNT(*) as transactions,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as current_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as prev_month_median,
                ROUND((100.0 * (
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                    - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                ) / NULLIF(
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                , 0))::NUMERIC, 1) as mom_change_pct,
                to_char(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month', 'FMMonth YYYY') as reporting_month
            FROM transactions
            WHERE ppd_type = 'A'
              AND record_status = 'A'
              AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
              AND UPPER(county) != 'GREATER LONDON'
            GROUP BY county

            UNION ALL

            -- London borough data
            SELECT
                district as area_name,
                'London Borough' as area_type,
                COUNT(*) as transactions,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as current_month_median,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month') as prev_month_median,
                ROUND((100.0 * (
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                    - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                ) / NULLIF(
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                        AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                , 0))::NUMERIC, 1) as mom_change_pct,
                to_char(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month', 'FMMonth YYYY') as reporting_month
            FROM transactions
            WHERE ppd_type = 'A'
              AND record_status = 'A'
              AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
              AND UPPER(county) = 'GREATER LONDON'
              AND district IS NOT NULL
            GROUP BY district
        )
        SELECT area_name, area_type, transactions, current_month_median, prev_month_median, mom_change_pct, reporting_month
        FROM area_data
        WHERE transactions > 10
          {area_filter}
        ORDER BY transactions DESC;
        """
        
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query)
            results = await cursor.fetchall()
            
        await conn.close()
        return [dict(row) for row in results]
        
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {e}")


def _format_row(row: Dict) -> str:
    area = row['area_name'] or 'Unknown'
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


async def generate_ai_summary(data: List[Dict]) -> str:
    """Send pre-selected notable data to Ollama for AI summary generation."""
    if not data:
        raise HTTPException(503, "No data available for summary")

    reporting_month = data[0].get('reporting_month') or 'Unknown'
    total_tx = sum(int(r['transactions']) for r in data)

    with_mom = [r for r in data if r['mom_change_pct'] is not None and r['current_month_median']]

    # Show all areas if small dataset, otherwise top 10
    top_by_volume = data if len(data) <= 15 else data[:10]
    top_gainers = sorted(with_mom, key=lambda r: float(r['mom_change_pct']), reverse=True)[:3]
    top_fallers = sorted(with_mom, key=lambda r: float(r['mom_change_pct']))[:3]

    def section(rows: List[Dict]) -> str:
        return "\n".join(f"  {_format_row(r)}" for r in rows)

    movers_block = ""
    if with_mom:
        movers_block = f"""
Biggest price increases (MoM):
{section(top_gainers)}

Biggest price falls (MoM):
{section(top_fallers)}"""
    else:
        movers_block = "\nNote: Month-over-month price comparison data is not yet available for this reporting period — do not comment on price direction."

    prompt = f"""You are a UK property market analyst writing a push notification briefing.

RULES:
- Use ONLY the numbers provided below. Do not invent any figures.
- If a median price is N/A, do not mention price for that area.
- Do not say prices are "not disclosed" or "unavailable" — just omit them.
- Write exactly 3 sentences. No more.

Sentence 1: State the reporting period, total transaction count, and number of areas monitored.
Sentence 2: Name the top 2-3 areas by transaction volume with their counts and median prices (if available).
Sentence 3: {('Name the biggest price gainer and faller with their MoM percentages.') if with_mom else ('Note that month-over-month price data is not yet available for this period.')}

DATA:
Reporting period: {reporting_month}
Total transactions: {total_tx:,}
Areas monitored: {len(data)}

Markets by volume:
{section(top_by_volume)}
{movers_block}"""

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
        data = await get_monthly_summary_data()
        summary = await generate_ai_summary(data)
        reporting_month = data[0].get('reporting_month') if data else None
        return {"summary": summary, "data_period": reporting_month, "areas_analysed": len(data)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Summary generation failed: {e}")
