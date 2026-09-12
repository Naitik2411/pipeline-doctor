from datetime import datetime 
from decimal import Decimal

from pydantic import BaseModel, Field

class Order(BaseModel):
    order_id: str
    customer_id: str
    order_total: Decimal = Field(..., max_digits=12, decimal_places=2)
    currency: str = 'USD'
    status: str
    ordered_at: datetime
    ingested_at: datetime|None = None

class SeedRequest(BaseModel):
    days: int|None = None
    orders_per_day: int | None = None
    reset: bool = False

class SeedResponse(BaseModel):
    days: int
    orders_per_day: int
    rows_inserted: int
    start_date: str
    end_date: str