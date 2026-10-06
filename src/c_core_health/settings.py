from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CCH_", env_file=".env", extra="ignore", frozen=True
    )

    pg_dsn: SecretStr = Field(
        description="SQLAlchemy URL, e.g. postgresql+psycopg://user:pass@host:5433/holmes"
    )
    statement_timeout_ms: int = 30_000

    prefect_api_url: str = "http://localhost:4200/api"
    prefect_api_key: SecretStr | None = None
    prefect_timeout_s: float = 15.0

    sar_collection_patterns: list[str] = Field(
        default=["%-s1", "%-rcm"],
        description="SQL LIKE patterns selecting SAR scene collections in pgstac",
    )
    default_since_days: int = 7
    max_page_size: int = 200

    scheduler_flow_names: list[str] = Field(
        default=[
            "aoi-trigger-for-s1",
            "aoi-triggers",
            "search-rcm-scene",
            "process-unprocessed-rcm-scenes-for-maritime-surveillance",
            "process-rcm-scenes-by-eodms-search-for-maritime-surveillance",
        ],
        description="Multi-scene flows; lineage walks stop below these",
    )
    parent_flow_names: list[str] = Field(
        default=["fast-sar-detector-processing-chain", "download-rcm-scene"],
        description="Preferred per-scene parent flows, in priority order",
    )
    rerun_parameter_overrides: dict[str, Any] = Field(
        default={"force": True},
        description="Applied only when the deployment's parameter schema declares the key",
    )
    active_state_types: list[str] = [
        "SCHEDULED",
        "PENDING",
        "RUNNING",
        "CANCELLING",
        "PAUSED",
    ]
    max_lineage_runs: int = 500

    enable_mutations: bool = False
    api_key: SecretStr | None = None
    backup_dir: Path = Path("backups")

    cors_origins: list[str] = []

    host: str = "127.0.0.1"
    port: int = 8000
    root_path: str = Field(
        default="",
        description="URL prefix the gateway strips, e.g. /health; keeps /docs working",
    )
    log_level: str = "INFO"
    log_json: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()  # pyright: ignore[reportCallIssue]
