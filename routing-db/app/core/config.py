"""routing-db/app/core/config.py"""
from __future__ import annotations

import os
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql+asyncpg://sentinel:changeme@postgres:5432/sentinel"
    db_pool_min:  int = 2
    db_pool_max:  int = 20
    sql_echo:     bool = False

    # API security
    admin_token: str = ""      # X-Admin-Token for write endpoints
    read_token:  str = ""      # X-Read-Token for read endpoints (optional — empty = public)

    # Service identity
    service_name: str = "routing-db"
    port:         int = 8000
    log_level:    str = "INFO"

    # CORS — restrict to dashboard origin in production
    cors_origins: list[str] = ["http://localhost:3000", "http://dashboard:80"]

    # Pagination
    default_page_size: int = 50
    max_page_size:     int = 200


@lru_cache
def get_settings() -> Settings:
    return Settings()
