from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from services.fault_injector.state import read_state
from services.mock_api.config import settings
from services.pipeline.metadata import collect_table_metadata, to_dagster_metadata
from services.pipeline.resources import PostgresResource


@asset(group_name="pipeline", description="Raw commerce orders already loaded into Postgres")
def raw_orders(context: AssetExecutionContext, postgres: PostgresResource) -> MaterializeResult:
    """
    Phase 1 already seeded raw.orders.
    This asset materializes by ensuring the table exists and attaching metadata
    so Dagster has lineage + run history for the agent.
    """
    engine = postgres.get_engine()

    from services.mock_api.db import init_raw_orders_table

    init_raw_orders_table()

    meta = collect_table_metadata(
        engine, "raw", "orders", ts_column="ordered_at", tz=settings.business_timezone
    )
    context.log.info(f"raw.orders rows={meta['row_count']} hash={meta['schema_hash']}")
    return MaterializeResult(metadata=to_dagster_metadata(meta))


@asset(
    deps=[raw_orders],
    group_name="pipeline",
    description="Clean + dedupe orders for analytics",
)
def staging_orders(context: AssetExecutionContext, postgres: PostgresResource) -> MaterializeResult:
    engine = postgres.get_engine()
    state = read_state()
    active = state.get("active")

    # Class 3: upstream renamed order_total → total_amount; staging still expects
    # order_total, so we load NULLs (signature) instead of crashing the SELECT.
    if active == "schema_drift":
        amount_expr = "NULL::numeric AS order_total"
        context.log.warning(
            "schema_drift active: mapping order_total → NULL (column was renamed upstream)"
        )
    else:
        amount_expr = "order_total"

    # order_total is nullable so schema-drift nulls can land (signature for the agent)
    ddl = """
    CREATE TABLE IF NOT EXISTS staging.orders (
        order_id     TEXT           NOT NULL PRIMARY KEY,
        customer_id  TEXT           NOT NULL,
        order_total  NUMERIC(12, 2),
        currency     TEXT           NOT NULL,
        status       TEXT           NOT NULL,
        ordered_at   TIMESTAMPTZ    NOT NULL,
        ingested_at  TIMESTAMPTZ    NOT NULL
    );
    """
    # Existing DBs may still have NOT NULL from earlier DDL — relax it.
    relax_null = """
    ALTER TABLE staging.orders ALTER COLUMN order_total DROP NOT NULL;
    """
    transform = f"""
    TRUNCATE staging.orders;
    INSERT INTO staging.orders (
        order_id, customer_id, order_total, currency, status, ordered_at, ingested_at
    )
    SELECT DISTINCT ON (order_id)
        order_id, customer_id, {amount_expr}, currency, status, ordered_at, ingested_at
    FROM raw.orders
    ORDER BY order_id, ingested_at DESC;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
        try:
            conn.exec_driver_sql(relax_null)
        except Exception as exc:  # table might be brand-new without the constraint
            context.log.debug(f"ALTER order_total nullability skipped: {exc}")
        conn.exec_driver_sql(transform)

    meta = collect_table_metadata(
        engine, "staging", "orders", ts_column="ordered_at", tz=settings.business_timezone
    )
    context.log.info(
        f"staging.orders rows={meta['row_count']} fault_active={active!r} "
        f"order_total_null_rate={meta['null_rates'].get('order_total')}"
    )
    return MaterializeResult(metadata=to_dagster_metadata(meta))


@asset(
    deps=[staging_orders],
    group_name="pipeline",
    description="Daily revenue aggregated in business timezone",
)
def daily_revenue(context: AssetExecutionContext, postgres: PostgresResource) -> MaterializeResult:
    engine = postgres.get_engine()
    tz = settings.business_timezone
    state = read_state()
    active = state.get("active")

    # Class 4: bucket by UTC instead of local business timezone (shape shift, same volume)
    use_utc = active == "timezone_bug"
    tz_sql = "UTC" if use_utc else tz
    if use_utc:
        context.log.warning("timezone_bug active: aggregating with AT TIME ZONE 'UTC'")

    ddl = """
    CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
        revenue_date   DATE           PRIMARY KEY,
        order_count    BIGINT         NOT NULL,
        gross_revenue  NUMERIC(14, 2) NOT NULL,
        paid_revenue   NUMERIC(14, 2) NOT NULL
    );
    """
    transform = f"""
    TRUNCATE analytics.daily_revenue;
    INSERT INTO analytics.daily_revenue (revenue_date, order_count, gross_revenue, paid_revenue)
    SELECT
        (ordered_at AT TIME ZONE '{tz_sql}')::date AS revenue_date,
        COUNT(*) AS order_count,
        COALESCE(SUM(order_total), 0) AS gross_revenue,
        COALESCE(SUM(order_total) FILTER (WHERE status = 'paid'), 0) AS paid_revenue
    FROM staging.orders
    GROUP BY 1
    ORDER BY 1;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
        conn.exec_driver_sql(transform)

    meta = collect_table_metadata(
        engine, "analytics", "daily_revenue", ts_column="revenue_date", tz=tz
    )

    with engine.connect() as conn:
        from sqlalchemy import text

        series = (
            conn.execute(
                text(
                    "SELECT revenue_date::text AS d, paid_revenue::float AS v "
                    "FROM analytics.daily_revenue ORDER BY revenue_date"
                )
            )
            .mappings()
            .all()
        )
        meta["daily_paid_revenue"] = {r["d"]: r["v"] for r in series}

    md = to_dagster_metadata(meta)
    md["daily_paid_revenue"] = MetadataValue.json(meta["daily_paid_revenue"])
    md["aggregation_timezone"] = MetadataValue.text(tz_sql)
    context.log.info(
        f"analytics.daily_revenue days={meta['row_count']} aggregation_tz={tz_sql}"
    )
    return MaterializeResult(metadata=md)
