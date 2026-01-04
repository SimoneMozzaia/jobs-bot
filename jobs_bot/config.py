from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Application configuration loaded from environment variables."""

    # Notion (optional if SYNC_TO_NOTION=0)
    notion_token: str
    notion_version: str
    notion_data_source_id: str

    # MySQL
    mysql_host: str
    mysql_port: int
    mysql_db: str
    mysql_user: str
    mysql_password: str

    # Runtime
    sync_limit: int = 50
    fit_min: int = 60
    max_calls_per_day: int = 50

    request_timeout_s: int = 20
    greenhouse_per_page: int = 100
    greenhouse_max_pages: int = 50

    # Ingestion controls
    ingest_per_source_limit: int = 0  # 0 = no limit
    max_fetch_per_run: int = 50
    max_new_jobs_per_day: int = 200
    sync_to_notion: int = 1

    @property
    def mysql_url(self) -> str:
        """SQLAlchemy URL for MySQL with utf8mb4."""
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
            f"?charset=utf8mb4"
        )


def _parse_int(name: str, raw: str | None, default: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {raw!r}") from exc


def validate_settings(s: Settings) -> None:
    """Fail-fast validation for numeric ranges and basic consistency."""
    errors: list[str] = []

    def check_int(
        name: str,
        value: int,
        *,
        min_v: int | None = None,
        max_v: int | None = None,
    ) -> None:
        if min_v is not None and value < min_v:
            errors.append(f"{name} must be >= {min_v} (got {value})")
        if max_v is not None and value > max_v:
            errors.append(f"{name} must be <= {max_v} (got {value})")

    check_int("MYSQL_PORT", s.mysql_port, min_v=1, max_v=65535)

    check_int("SYNC_LIMIT", s.sync_limit, min_v=1, max_v=500)
    check_int("FIT_MIN", s.fit_min, min_v=0, max_v=100)

    # 0 = unlimited for per-day caps is allowed (implemented in api_usage)
    check_int("MAX_CALLS_PER_DAY", s.max_calls_per_day, min_v=0, max_v=10_000)
    check_int("MAX_NEW_JOBS_PER_DAY", s.max_new_jobs_per_day, min_v=0, max_v=50_000)

    # max_fetch_per_run is used as a hard cap per run; keep it >= 1
    check_int("MAX_FETCH_PER_RUN", s.max_fetch_per_run, min_v=1, max_v=50_000)

    check_int("REQUEST_TIMEOUT_S", s.request_timeout_s, min_v=1, max_v=120)
    check_int("GREENHOUSE_PER_PAGE", s.greenhouse_per_page, min_v=1, max_v=500)
    check_int("GREENHOUSE_MAX_PAGES", s.greenhouse_max_pages, min_v=1, max_v=500)

    check_int("INGEST_PER_SOURCE_LIMIT", s.ingest_per_source_limit, min_v=0, max_v=1_000_000)

    if s.sync_to_notion not in (0, 1):
        errors.append(f"SYNC_TO_NOTION must be 0 or 1 (got {s.sync_to_notion})")

    # Notion becomes required only if sync enabled
    if s.sync_to_notion == 1:
        if not s.notion_token.strip():
            errors.append("NOTION_TOKEN is required when SYNC_TO_NOTION=1")
        if not s.notion_data_source_id.strip():
            errors.append("NOTION_DATA_SOURCE_ID is required when SYNC_TO_NOTION=1")

    if errors:
        raise RuntimeError("Invalid configuration: " + "; ".join(errors))


def get_settings() -> Settings:
    load_dotenv()

    def req(name: str) -> str:
        v = os.getenv(name)
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v

    sync_to_notion = _parse_int("SYNC_TO_NOTION", os.getenv("SYNC_TO_NOTION"), 1)

    # Notion envs are required only when sync_to_notion=1
    notion_token = os.getenv("NOTION_TOKEN", "") if sync_to_notion == 0 else req("NOTION_TOKEN")
    notion_data_source_id = (
        os.getenv("NOTION_DATA_SOURCE_ID", "") if sync_to_notion == 0 else req("NOTION_DATA_SOURCE_ID")
    )
    notion_version = os.getenv("NOTION_VERSION", "2025-09-03")

    settings = Settings(
        notion_token=notion_token,
        notion_version=notion_version,
        notion_data_source_id=notion_data_source_id,
        mysql_host=req("MYSQL_HOST"),
        mysql_port=_parse_int("MYSQL_PORT", os.getenv("MYSQL_PORT"), 3306),
        mysql_db=req("MYSQL_DB"),
        mysql_user=req("MYSQL_USER"),
        mysql_password=req("MYSQL_PASSWORD"),
        sync_limit=_parse_int("SYNC_LIMIT", os.getenv("SYNC_LIMIT"), 50),
        fit_min=_parse_int("FIT_MIN", os.getenv("FIT_MIN"), 60),
        max_calls_per_day=_parse_int("MAX_CALLS_PER_DAY", os.getenv("MAX_CALLS_PER_DAY"), 50),
        request_timeout_s=_parse_int("REQUEST_TIMEOUT_S", os.getenv("REQUEST_TIMEOUT_S"), 20),
        greenhouse_per_page=_parse_int("GREENHOUSE_PER_PAGE", os.getenv("GREENHOUSE_PER_PAGE"), 100),
        greenhouse_max_pages=_parse_int("GREENHOUSE_MAX_PAGES", os.getenv("GREENHOUSE_MAX_PAGES"), 50),
        ingest_per_source_limit=_parse_int("INGEST_PER_SOURCE_LIMIT", os.getenv("INGEST_PER_SOURCE_LIMIT"), 0),
        max_fetch_per_run=_parse_int("MAX_FETCH_PER_RUN", os.getenv("MAX_FETCH_PER_RUN"), 50),
        max_new_jobs_per_day=_parse_int("MAX_NEW_JOBS_PER_DAY", os.getenv("MAX_NEW_JOBS_PER_DAY"), 200),
        sync_to_notion=sync_to_notion,
    )

    validate_settings(settings)
    return settings
