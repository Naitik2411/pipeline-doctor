from dagster import ConfigurableResource
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class PostgresResource(ConfigurableResource):
    database_url: str

    def get_engine(self) -> Engine:
        return create_engine(self.database_url, pool_pre_ping=True)
    def execute(self, sql: str, params:dict|None = None):
        with self.get_engine().begin() as conn:
            return conn.execute(text(sql), params or {})


# class PostgresResource(ConfigurableResource):
#     database_url: str

#     def setup_for_execution(self, context):
#         self._engine = create_engine(
#             self.database_url,
#             pool_pre_ping=True
#         )

#     def get_engine(self) -> Engine:
#         return self._engine

#     def execute(self, sql: str, params: dict | None = None):
#         with self._engine.begin() as conn:
#             return conn.execute(
#                 text(sql),
#                 params or {}
#             )