from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://concurrenseat:concurrenseat@localhost:5433/concurrenseat"
    redis_url: str = "redis://localhost:6379/0"

    anthropic_api_key: str = ""
    anthropic_model: str = "claude-opus-4-8"

    default_strategy: str = "optimistic"
    environment: str = "development"
    # Comma-separated allowed browser origins (deployed frontend + local dev).
    cors_origins: str = "http://localhost:5173"


@lru_cache
def get_settings() -> Settings:
    return Settings()
