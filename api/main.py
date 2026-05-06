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
        
        query = """
        SELECT
            county,
            COUNT(*) as transactions,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
            ROUND(100.0 * (
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
                - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                    AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
            ) / NULLIF(
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
                    AND date < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
            , 0), 1) as mom_change_pct
        FROM transactions
        WHERE ppd_type = 'A'
          AND record_status = 'A'
          AND date >= date_trunc('month', CURRENT_DATE) - INTERVAL '2 months'
        GROUP BY county
        HAVING COUNT(*) > 10
        ORDER BY transactions DESC;
        """
        
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query)
            results = await cursor.fetchall()
            
        await conn.close()
        return [dict(row) for row in results]
        
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {e}")


async def generate_ai_summary(data: List[Dict]) -> str:
    """Send data to Ollama for AI summary generation."""
    if not data:
        raise HTTPException(503, "No data available for summary")
    
    # Format data for the prompt
    formatted_data = ""
    for row in data:
        county = row['county'] or 'Unknown'
        transactions = int(row['transactions'])
        median_price = int(row['median_price']) if row['median_price'] else 0
        mom_change = float(row['mom_change_pct']) if row['mom_change_pct'] is not None else 0
        
        formatted_data += f"\n{county}: {transactions:,} transactions, median £{median_price:,}, {mom_change:+.1f}% MoM change"
    
    prompt = f"""You are a UK property market analyst. Given the following monthly data, write a 3-5 sentence plain english briefing suitable for a push notification. Be specific with numbers. Do not speculate beyond the data.

Data:{formatted_data}"""
    
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False
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
        # Get data from database
        data = await get_monthly_summary_data()
        
        # Generate AI summary
        summary = await generate_ai_summary(data)
        
        return {"summary": summary}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Summary generation failed: {e}")
