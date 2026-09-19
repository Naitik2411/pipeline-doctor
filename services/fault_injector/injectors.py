from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from services.fault_injector.state import clear_state, write_state
from services.mock_api.config import settings
from typing import Any

TZ =ZoneInfo(settings.business_timezone)

def _engine():
    return create_engine(settings.database_url, pool_pre_ping=True)

def _target_day(day: date | None= None) -> date:
    """Default : yesterday in business timezone"""
    if day:
        return day
    return datetime.now(TZ).date() - timedelta(days=1)

def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=TZ)
    end = start + timedelta(days=1)
    return start,end


#Class 1 : Missing batch
def inject_missing_batch(day:date|None=None) -> dict:
    """Delete all raw orders for that one local business day"""
    day = _target_day(day)
    start, end = _day_bounds(day)
    eng = _engine()

    with eng.begin() as conn:
        result = conn.execute(
            text(
                """DELETE FROM raw.orders WHERE ordered_at >= :start AND ordered_at < :end"""
            ), {"start":start, "end":end}
        )
        deleted = result.rowcount
    
    write_state("missing_batch", {"day": day.isoformat(), "deleted":deleted})
    return{
        "class":"missing_batch",
        "day": day.isoformat(),
        "deleted_rows":deleted,
        "expected_signature":"zero raw rows for that date; daily_revenue drops"
    }

#Class 2: Duplicate records

def inject_duplicates(day:date|None=None) -> dict[str,Any]:
    """Re-insert the saem order_ids for a day with a new ingested_at"""
    day = _target_day(date)
    start, end = _day_bounds(day)
    now = datetime.now(timezone.utc)
    eng = _engine()

    with eng.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO raw.orders (
                    order_id, customer_id, order_total, currency,
                    status, ordered_at, ingested_at
                )
                SELECT
                    order_id, customer_id, order_total, currency,
                    status, ordered_at, :now
                FROM raw.orders
                WHERE ordered_at >= :start AND ordered_at < :end
                """
            ),
            {"start": start, "end": end, "now": now},
        )
        inserted = result.rowcount
    write_state("duplicates", {"day": day.isoformat(), "inserted": inserted})
    return {
        "class": "duplicates",
        "day": day.isoformat(),
        "inserted_rows": inserted,
        "expected_signature": (
            "raw row count ~2x for that day; COUNT(DISTINCT order_id) unchanged"
        ),
    }

#Class 3 : Schema drift
def inject_schema_drift() -> dict:
    """
    Rename order_total to total_amount on raw.orders.
    Staging still selects order_total -> nulls/failures after remap helper
    """
    eng = _engine()
    with eng.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema="raw" and table_name="orders"
                AND column_name="order_total"
                """
            )        
        ).scalar()
        if exists:
            conn.execute(
                text(
                    "ALTER TABLE raw.orders RENAME COLUMN order_total TO total_amount"
                )
            )
            renamed = True
        else:
            renamed = False
    write_state("schema_drift",{"renamed":renamed})
    return{
        "class":"schema_drift",
        "renamed":renamed,
        "expected_signature":"schema_hash changes; staging nulls on order_total",
    }

#Class 4: TimeZone bug:

def inject_timezone_bug() -> dict:
    """
    No data mutation -> flips a flag so daily_revenue buckets by UTC timezone.
    total volume unchanged and day/hour shape shifts.
    """
    write_state("timezone_bug", {"use_utc": True})
    return {
        "class": "timezone_bug",
        "expected_signature": (
            "same total sum/count; hourly_histogram / day shape shifts"
        ),
    }

# Class 5 -> Real business change (control)
def inject_business_change(day:date |None = None, drop_fraction:float = 0.4) -> dict:
    """
    Genuinely reduce demand for one day i.e. delete approx 40% of that day's orders.
    Pipeline stays healthy with no schema/dup/gap artifacts beyond the real drop 
    """
    day = _target_day(day)
    start, end = _day_bounds(day)
    eng = _engine()
    with eng.begin() as conn:
        result = conn.execute(
            text(
                """
                DELETE from raw.orders WHERE ctid IN (
                    SELECT ctid from raw.orders WHERE ordered_at>=:start and ordered_at<:end
                    ORDER BY order_id
                    LIMIT(
                        SELECT GREATEST(1, (COUNT(*)*:frac)::int) FROM raw.orders WHERE ordered_at >=:start and ordered_at<:end
                    )
                )
                """
            ), {"start":start, "end":end, "frac":drop_fraction},
        )
        deleted = result.rowcount
    write_state(
        "business_change",
        {"day": day.isoformat(), "deleted": deleted, "drop_fraction": drop_fraction},
    )
    return {
        "class": "business_change",
        "day": day.isoformat(),
        "deleted_rows": deleted,
        "expected_signature": (
            "revenue down; stable schema_hash; no dup spike; no rename; no tz flag"
        ),
    }

# ── Reset ───────────────────────────────────────────────────────────
def reset_faults(reseed: bool = True) -> dict:
    """
    Undo injector effects.
    Easiest reliable reset: restore schema if needed, then re-seed raw.orders.
    """
    eng = _engine()
    with eng.begin() as conn:
        # undo schema drift if present
        has_total_amount = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='raw' AND table_name='orders'
                  AND column_name='total_amount'
                """
            )
        ).scalar()
        has_order_total = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='raw' AND table_name='orders'
                  AND column_name='order_total'
                """
            )
        ).scalar()
        if has_total_amount and not has_order_total:
            conn.execute(
                text(
                    "ALTER TABLE raw.orders RENAME COLUMN total_amount TO order_total"
                )
            )
    clear_state()
    if reseed:
        from services.mock_api.seed import seed_raw_orders
        seed_result = seed_raw_orders(reset=True)
    else:
        seed_result = None
    return {"reset": True, "reseed": seed_result}