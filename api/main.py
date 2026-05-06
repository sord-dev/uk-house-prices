import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse

sys.path.insert(0, "/app")
from ingest import LandRegistryIngestor

jobs: dict = {}

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
