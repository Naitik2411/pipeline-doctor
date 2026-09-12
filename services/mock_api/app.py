from contextlib import asynccontextmanager
from datetime import date
from fastapi import FastAPI, HTTPException, Query
from services.mock_api.db import init_raw_orders_table
from services.mock_api.generator import generate_orders_for_day
from services.mock_api.models import Order, SeedRequest, SeedResponse
from services.mock_api.seed import seed_raw_orders

@asynccontextmanager
async def lifespan(app:FastAPI):
    init_raw_orders_table()
    yield

app = FastAPI(title="PipelineDoctor Commerce API", version="0.1.0", lifespan=lifespan)

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

@app.get("/orders", response_model=list[Order])
def get_orders(
    day: date = Query(..., description="Local business date, YYYY-MM-DD"),
    n: int = Query(100, ge=1, le=5000),
) -> list[Order]:
    """Generate a day's orders on the fly (source-system style)."""
    return generate_orders_for_day(day, n)

@app.post("/admin/seed", response_model=SeedResponse)
def admin_seed(body: SeedRequest) -> SeedResponse:
    """Write historical orders into raw.orders for the pipeline."""
    try:
        result = seed_raw_orders(
            days=body.days,
            orders_per_day=body.orders_per_day,
            reset=body.reset,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return SeedResponse(**result)