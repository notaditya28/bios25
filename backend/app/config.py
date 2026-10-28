from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: Literal["development", "test", "production"] = "development"
    app_base_url: str = "http://localhost:3000"
    secret_key: str = Field(min_length=16)
    log_level: str = "INFO"

    database_url: str
    redis_url: str = "redis://redis:6379/0"

    session_ttl_hours: int = 336
    cookie_secure: bool = False
    login_rate_limit_per_15min: int = 10
    verify_token_ttl_hours: int = 48
    reset_token_ttl_minutes: int = 60

    email_backend: Literal["smtp", "postmark"] = "smtp"
    email_from: str = "TradeRadar <alerts@example.com>"
    smtp_host: str = "mailpit"
    smtp_port: int = 1025
    postmark_server_token: str = ""

    source_http_timeout_s: float = 120.0
    source_user_agent: str = "TradeRadar/0.1"
    source_cache_dir: str = "/data/cache"

    eu_fsf_token: str = ""
    # Matches scoring at or above this (0-100) go to the human review queue.
    screening_review_threshold: float = 85.0

    allow_dev_seed: bool = False

    @model_validator(mode="after")
    def _production_guards(self) -> "Settings":
        if self.app_env == "production":
            if not self.cookie_secure:
                raise ValueError("COOKIE_SECURE must be true in production")
            if self.secret_key.startswith("change-me"):
                raise ValueError("SECRET_KEY must be set in production")
            if self.allow_dev_seed:
                raise ValueError("ALLOW_DEV_SEED must be false in production")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
