import psycopg
from fastapi import APIRouter, BackgroundTasks, HTTPException

from .config import DB_CONFIG  # noqa: F401 — also triggers sys.path setup for ingest import
from .state import _summary_cache, jobs
from ingest import LandRegistryIngestor

router = APIRouter()


@router.post("/ingest/monthly", status_code=202)
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


@router.post("/ingest/yearly", status_code=202)
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


@router.post("/ingest/backfill", status_code=202)
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


@router.get("/ingest/coverage")
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


@router.get("/ingest/{job_id}/status")
async def job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]
