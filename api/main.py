from fastapi import FastAPI

from .ingest import router as ingest_router
from .summarise import router as summarise_router

app = FastAPI(root_path="/api", title="UK House Prices API")

app.include_router(ingest_router)
app.include_router(summarise_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
