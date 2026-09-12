from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = (
        "postgresql://pipelinedoctor:pipelinedoctor@localhost:5432/pipelinedoctor"
    )

    seed_days: int = 56  # ~8 weeks
    orders_per_day: int = 2000  # start small locally
    faker_seed: int = 42
    business_timezone: str = "America/New_York"


settings = Settings()
