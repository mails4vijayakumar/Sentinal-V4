from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # SNOW (already used by Agents 1, 3-7; reuse env names)
    snow_base_url: str = Field(alias="SNOW_BASE_URL")
    snow_client_id: str = Field(alias="SNOW_CLIENT_ID")
    snow_client_secret: str = Field(alias="SNOW_CLIENT_SECRET")
    snow_api_timeout_seconds: int = Field(default=30, alias="SNOW_API_TIMEOUT_SECONDS")

    # Shared infra
    database_url: str = Field(alias="DATABASE_URL")
    ollama_base_url: str = Field(default="http://ollama:11434", alias="OLLAMA_BASE_URL")
    embed_model: str = Field(default="nomic-embed-text", alias="EMBED_MODEL")
    llm_provider: str = Field(default="ollama", alias="LLM_PROVIDER")

    # Agent 8 specific
    synth_schedule_cron: str = Field(default="0 2 1 * *", alias="SYNTH_SCHEDULE_CRON")
    synth_min_cluster_size: int = Field(default=5, alias="SYNTH_MIN_CLUSTER_SIZE")
    synth_min_cluster_cohesion: float = Field(default=0.65, alias="SYNTH_MIN_CLUSTER_COHESION")
    synth_quality_score_floor: float = Field(default=0.40, alias="SYNTH_QUALITY_SCORE_FLOOR")
    synth_dedup_update_threshold: float = Field(default=0.92, alias="SYNTH_DEDUP_UPDATE_THRESHOLD")
    synth_dedup_review_threshold: float = Field(default=0.80, alias="SYNTH_DEDUP_REVIEW_THRESHOLD")
    synth_llm_model: str = Field(default="llama3.1:70b", alias="SYNTH_LLM_MODEL")
    synth_llm_max_tokens_per_run: int = Field(default=500_000, alias="SYNTH_LLM_MAX_TOKENS_PER_RUN")
    synth_publish_confluence: bool = Field(default=True, alias="SYNTH_PUBLISH_CONFLUENCE")
    synth_confluence_space: str = Field(default="AUTO_KB", alias="SYNTH_CONFLUENCE_SPACE")
    synth_retire_low_feedback: bool = Field(default=True, alias="SYNTH_RETIRE_LOW_FEEDBACK")
    synth_admin_token: str = Field(default="", alias="SYNTH_ADMIN_TOKEN")
    synth_max_concurrent_synthesize: int = Field(default=10, alias="SYNTH_MAX_CONCURRENT_SYNTHESIZE")

    # Confluence (already used by Agent 6)
    confluence_base_url: str = Field(default="", alias="CONFLUENCE_BASE_URL")
    confluence_token: str = Field(default="", alias="CONFLUENCE_TOKEN")


def get_settings() -> Settings:
    return Settings()
