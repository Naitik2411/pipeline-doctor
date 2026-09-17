from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any
from dagster import MetadataValue
from sqlalchemy import text
from sqlalchemy.engine import Engine

def schema_hash(engine: Engine, schema: str, table:str) -> str:
    rows = engine.execute(
        text(
            """SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = :schema and table_name = :table ORDER BY ordinal_position"""
        ),{"schema":schema, "table": table}
    ).mappings().all()

    payload = json.dumps([dict(r) for r in rows], sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]

def collect_table_metadata(engine:Engine, schema:str, table:str, ts_column: str="ordered_at", tz:str = "America/New_York",) -> dict[str, Any]:
    fq = f"{schema}.{table}"

    with engine.connect() as conn:
        cols = conn.execute(
            text(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
                """
            ),
            {"schema": schema, "table": table},
        ).mappings().all()
        sch = hashlib.sha256(
            json.dumps([dict(c) for c in cols], sort_keys=True).encode()
        ).hexdigest()[:16]
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fq}")).scalar_one()
        null_rates = {}
        for c in cols:
            col = c["column_name"]
            nulls = conn.execute(
                text(f"SELECT COUNT(*) FROM {fq} WHERE {col} IS NULL")
            ).scalar_one()
            null_rates[col] = round(nulls / row_count, 6) if row_count else 0.0
        ts_stats = conn.execute(
            text(
                f"""
                SELECT MIN({ts_column}) AS min_ts, MAX({ts_column}) AS max_ts
                FROM {fq}
                """
            )
        ).mappings().one()
        # Hourly histogram in business timezone — required for class-4 timezone bugs
        hist_rows = conn.execute(
            text(
                f"""
                SELECT EXTRACT(HOUR FROM ({ts_column} AT TIME ZONE :tz))::int AS hour,
                       COUNT(*) AS cnt
                FROM {fq}
                GROUP BY 1
                ORDER BY 1
                """
            ),
            {"tz": tz},
        ).mappings().all()
        hourly_histogram = {int(r["hour"]): int(r["cnt"]) for r in hist_rows}
    return {
        "row_count": int(row_count),
        "schema_hash": sch,
        "null_rates": null_rates,
        "min_ts": ts_stats["min_ts"],
        "max_ts": ts_stats["max_ts"],
        "hourly_histogram": hourly_histogram,
    }

def to_dagster_metadata(meta:dict[str, Any]) -> dict[str,Any]:
    """Convert plain dict -> Dagster metadata value objects for the ui"""
    return {
        "row_count": MetadataValue.int(meta["row_count"]),
        "schema_hash": MetadataValue.text(meta["schema_hash"]),
        "null_rates": MetadataValue.json(meta["null_rates"]),
        "min_ts": MetadataValue.text(str(meta["min_ts"])),
        "max_ts": MetadataValue.text(str(meta["max_ts"])),
        "hourly_histogram": MetadataValue.json(meta["hourly_histogram"]),
    }