from dagster import Definitions, EnvVar

from services.pipeline.assets import daily_revenue, raw_orders, staging_orders
from services.pipeline.resources import PostgresResource

defs = Definitions(
    assets=[raw_orders, staging_orders, daily_revenue],
    resources={
        "postgres": PostgresResource(
            database_url=EnvVar("DATABASE_URL"),
        )
    },
)