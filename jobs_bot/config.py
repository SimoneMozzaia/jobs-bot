from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Application configuration loaded from environment variables."""

    # Notion (required only if SYNC_TO_NOTION=1)
    notion_token: str = ""
    notion_version: str = "2025-09-03"
    notion_data_source_id: str = ""

    # MySQL
    mysql_host: str = ""
    mysql_port: int = 3306
    mysql_db: str = ""
    mysql_user: str = ""
    mysql_password: str = ""

    # Runtime
    sync_limit: int = 50
    fit_min: int = 60
    max_calls_per_day: int = 50
    max_new_jobs_per_day: int = 200
    max_fetch_per_run: int = 50

    request_timeout_s: int = 20
    greenhouse_per_page: int = 100
    greenhouse_max_pages: int = 50

    ingest_per_source_limit: int = 0  # 0 = no limit
    sync_to_notion: int = 1

    # LLM enrichment
    enrich_with_llm: int = 0
    enrich_limit: int = 10
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_base_url: str = "https://api.openai.com/v1"

    # Multi-profile (Solution B)
    profile_id: str = "default"
    profiles_dir: str = ""  # empty = disabled (keeps backward compatibility)

    @property
    def mysql_url(self) -> str:
        """SQLAlchemy URL for MySQL with utf8mb4."""
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
            f"?charset=utf8mb4"
        )

    @property
    def profile_cv_path(self) -> str | None:
        """
        Fixed CV path for the active profile.

        Convention:
          {PROFILES_DIR}/{PROFILE_ID}/cv.docx
        """
        if not self.profiles_dir:
            return None
        return str(Path(self.profiles_dir) / self.profile_id / "cv.docx")


def validate_settings(s: Settings) -> None:
    """Fail-fast validation for numeric ranges and basic consistency."""
    errors: list[str] = []

    def check_int(name: str, value: int, *, min_v: int | None = None, max_v: int | None = None) -> None:
        if min_v is not None and value < min_v:
            errors.append(f"{name} must be >= {min_v} (got {value})")
        if max_v is not None and value > max_v:
            errors.append(f"{name} must be <= {max_v} (got {value})")

    def check_required(name: str, value: str) -> None:
        if not value:
            errors.append(f"Missing required env var: {name}")

    check_required("MYSQL_HOST", s.mysql_host)
    check_required("MYSQL_DB", s.mysql_db)
    check_required("MYSQL_USER", s.mysql_user)
    check_required("MYSQL_PASSWORD", s.mysql_password)

    check_int("MYSQL_PORT", s.mysql_port, min_v=1, max_v=65535)

    check_int("SYNC_LIMIT", s.sync_limit, min_v=1, max_v=5000)
    check_int("FIT_MIN", s.fit_min, min_v=0, max_v=100)

    check_int("MAX_CALLS_PER_DAY", s.max_calls_per_day, min_v=0, max_v=1_000_000)
    check_int("MAX_NEW_JOBS_PER_DAY", s.max_new_jobs_per_day, min_v=0, max_v=1_000_000)
    check_int("MAX_FETCH_PER_RUN", s.max_fetch_per_run, min_v=1, max_v=1_000_000)

    check_int("REQUEST_TIMEOUT_S", s.request_timeout_s, min_v=1, max_v=300)
    check_int("GREENHOUSE_PER_PAGE", s.greenhouse_per_page, min_v=1, max_v=500)
    check_int("GREENHOUSE_MAX_PAGES", s.greenhouse_max_pages, min_v=1, max_v=10_000)

    check_int("INGEST_PER_SOURCE_LIMIT", s.ingest_per_source_limit, min_v=0, max_v=10_000_000)

    if s.sync_to_notion not in (0, 1):
        errors.append(f"SYNC_TO_NOTION must be 0 or 1 (got {s.sync_to_notion})")
    if s.sync_to_notion == 1:
        check_required("NOTION_TOKEN", s.notion_token)
        check_required("NOTION_DATA_SOURCE_ID", s.notion_data_source_id)

    if s.enrich_with_llm not in (0, 1):
        errors.append(f"ENRICH_WITH_LLM must be 0 or 1 (got {s.enrich_with_llm})")
    if s.enrich_with_llm == 1:
        check_required("OPENAI_API_KEY", s.openai_api_key)
        check_int("ENRICH_LIMIT", s.enrich_limit, min_v=1, max_v=10_000)

    # Multi-profile sanity
    if not s.profile_id or len(s.profile_id) > 64:
        errors.append("PROFILE_ID must be a non-empty string up to 64 chars")
    if any(sep in s.profile_id for sep in ("/", "\\", "..")):
        errors.append("PROFILE_ID must not contain path separators or '..'")

    if s.profiles_dir:
        p = Path(s.profiles_dir)
        if not p.exists():
            errors.append(f"PROFILES_DIR does not exist: {s.profiles_dir}")
        elif not p.is_dir():
            errors.append(f"PROFILES_DIR is not a directory: {s.profiles_dir}")

        cv = Path(s.profile_cv_path or "")
        if not cv.exists():
            errors.append(f"Missing CV for profile: {cv}")
        elif not cv.is_file():
            errors.append(f"CV path is not a file: {cv}")

    if errors:
        raise RuntimeError("Invalid configuration: " + "; ".join(errors))


def get_settings() -> Settings:
    load_dotenv()

    settings = Settings(
        mysql_host=os.getenv("MYSQL_HOST", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_db=os.getenv("MYSQL_DB", ""),
        mysql_user=os.getenv("MYSQL_USER", ""),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        sync_limit=int(os.getenv("SYNC_LIMIT", "50")),
        fit_min=int(os.getenv("FIT_MIN", "60")),
        max_calls_per_day=int(os.getenv("MAX_CALLS_PER_DAY", "50")),
        max_new_jobs_per_day=int(os.getenv("MAX_NEW_JOBS_PER_DAY", "200")),
        max_fetch_per_run=int(os.getenv("MAX_FETCH_PER_RUN", "50")),
        request_timeout_s=int(os.getenv("REQUEST_TIMEOUT_S", "20")),
        greenhouse_per_page=int(os.getenv("GREENHOUSE_PER_PAGE", "100")),
        greenhouse_max_pages=int(os.getenv("GREENHOUSE_MAX_PAGES", "50")),
        ingest_per_source_limit=int(os.getenv("INGEST_PER_SOURCE_LIMIT", "0")),
        sync_to_notion=int(os.getenv("SYNC_TO_NOTION", "1")),
        notion_token=os.getenv("NOTION_TOKEN", ""),
        notion_version=os.getenv("NOTION_VERSION", "2025-09-03"),
        notion_data_source_id=os.getenv("NOTION_DATA_SOURCE_ID", ""),
        enrich_with_llm=int(os.getenv("ENRICH_WITH_LLM", "0")),
        enrich_limit=int(os.getenv("ENRICH_LIMIT", "10")),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        profile_id=os.getenv("PROFILE_ID", "default"),
        profiles_dir=os.getenv("PROFILES_DIR", ""),
    )

    validate_settings(settings)
    return settings
