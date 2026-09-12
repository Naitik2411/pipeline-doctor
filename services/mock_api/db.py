from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.mock_api.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_raw_orders_table() -> None:
    """Create raw.orders if missing. Schemas come from infra/init-db.sql."""
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.orders (
        order_id        TEXT           NOT NULL,
        customer_id     TEXT           NOT NULL,
        order_total     NUMERIC(12, 2) NOT NULL,
        currency        TEXT           NOT NULL DEFAULT 'USD',
        status          TEXT           NOT NULL,
        ordered_at      TIMESTAMPTZ    NOT NULL,
        ingested_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
        PRIMARY KEY (order_id, ingested_at)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_orders_ordered_at
        ON raw.orders (ordered_at);

    CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id
        ON raw.orders (order_id);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
