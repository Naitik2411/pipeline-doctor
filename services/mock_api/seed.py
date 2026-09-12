from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from services.mock_api.config import settings
from services.mock_api.db import SessionLocal, engine, init_raw_orders_table
from services.mock_api.generator import generate_history


def seed_raw_orders(
    days: int | None = None,
    orders_per_day: int | None = None,
    reset: bool = False,
) -> dict:
    days = days or settings.seed_days
    orders_per_day = orders_per_day or settings.orders_per_day

    init_raw_orders_table()

    if reset:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.orders"))
    
    orders = generate_history(days=days, orders_per_day=orders_per_day,)
    now = datetime.now(timezone.utc)

    insert_sql = text(
        """
        INSERT INTO raw.orders (
            order_id, customer_id, order_total, currency, status, ordered_at, ingested_at
        ) VALUES (
            :order_id, :customer_id, :order_total, :currency, :status, :ordered_at, :ingested_at
        )
        ON CONFLICT (order_id, ingested_at) DO NOTHING
        """
    )

    rows = [
        {
            "order_id": o.order_id,
            "customer_id": o.customer_id,
            "order_total": o.order_total
            if isinstance(o.order_total, Decimal)
            else Decimal(str(o.order_total)),
            "currency": o.currency,
            "status": o.status,
            "ordered_at": o.ordered_at,
            "ingested_at": now,
        }
        for o in orders
    ]

    with SessionLocal() as session:
        chunk = 1000
        for i in range(0, len(rows), chunk):
            session.execute(insert_sql, rows[i:i+chunk])
        session.commit()
    
    end = date.today()
    start = end - timedelta(days=days - 1)
    return {
        "days": days,
        "orders_per_day": orders_per_day,
        "rows_inserted": len(rows),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }

if __name__ == "__main__":
    result = seed_raw_orders(reset=True)
    print(result)