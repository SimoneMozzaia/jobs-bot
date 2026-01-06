from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Application configuration loaded from environment variables."""

    # Notion (required only when sync_to_notion=1)
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

    # LLM enrichment (jobs -> job_enrichment)
    enrich_with_llm: int = 0
    enrich_limit: int = 10

    openai_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"
    openai_base_url: str = "https://api.openai.com/v1"

    # Multi-profile (Solution B)
    profile_id: str = "default"
    profiles_dir: str = ""  # empty = disabled (keeps backward compatibility)

    # ------------------------------------------------------------
    # Source discovery (CompaniesMarketCap -> Wikidata -> careers -> ATS)
    # ------------------------------------------------------------
    discovery_enable: int = 0
    discovery_regions: str = "US,EU,CH,CANADA,UAE"
    discovery_max_companies_per_region: int = 5000
    discovery_request_delay_s: float = 1.0
    discovery_max_sources_per_run: int = 500

    discovery_verify_enable: int = 0
    discovery_verify_max_per_run: int = 200

    discovery_user_agent: str = "jobs-bot/1.0 (+https://example.invalid)"
    wikidata_user_agent: str = "jobs-bot/1.0 (+https://example.invalid)"
    wikidata_name_fallback_enable: int = 0

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

        If PROFILES_DIR is empty, profiling is disabled and this returns None.
        """
        if not self.profiles_dir:
            return None
        return str(Path(self.profiles_dir) / self.profile_id / "cv.docx")


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

    check_int("MAX_CALLS_PER_DAY", s.max_calls_per_day, min_v=0, max_v=10_000)
    check_int("MAX_NEW_JOBS_PER_DAY", s.max_new_jobs_per_day, min_v=0, max_v=50_000)
    check_int("MAX_FETCH_PER_RUN", s.max_fetch_per_run, min_v=1, max_v=50_000)

    check_int("REQUEST_TIMEOUT_S", s.request_timeout_s, min_v=1, max_v=120)
    check_int("GREENHOUSE_PER_PAGE", s.greenhouse_per_page, min_v=1, max_v=500)
    check_int("GREENHOUSE_MAX_PAGES", s.greenhouse_max_pages, min_v=1, max_v=500)

    check_int(
        "INGEST_PER_SOURCE_LIMIT",
        s.ingest_per_source_limit,
        min_v=0,
        max_v=1_000_000,
    )

    if s.sync_to_notion not in (0, 1):
        errors.append(f"SYNC_TO_NOTION must be 0 or 1 (got {s.sync_to_notion})")

    # Discovery toggles
    if s.discovery_enable not in (0, 1):
        errors.append(f"DISCOVERY_ENABLE must be 0 or 1 (got {s.discovery_enable})")
    if s.discovery_verify_enable not in (0, 1):
        errors.append(
            "DISCOVERY_VERIFY_ENABLE must be 0 or 1 "
            f"(got {s.discovery_verify_enable})"
        )
    if s.wikidata_name_fallback_enable not in (0, 1):
        errors.append(
            "WIKIDATA_NAME_FALLBACK_ENABLE must be 0 or 1 "
            f"(got {s.wikidata_name_fallback_enable})"
        )

    check_int(
        "DISCOVERY_MAX_COMPANIES_PER_REGION",
        s.discovery_max_companies_per_region,
        min_v=1,
        max_v=50_000,
    )
    check_int(
        "DISCOVERY_MAX_SOURCES_PER_RUN",
        s.discovery_max_sources_per_run,
        min_v=1,
        max_v=50_000,
    )
    check_int(
        "DISCOVERY_VERIFY_MAX_PER_RUN",
        s.discovery_verify_max_per_run,
        min_v=1,
        max_v=50_000,
    )

    if not (s.discovery_regions or "").strip():
        errors.append("DISCOVERY_REGIONS must be a non-empty comma-separated string")

    if s.discovery_request_delay_s < 0 or s.discovery_request_delay_s > 30:
        errors.append(
            "DISCOVERY_REQUEST_DELAY_S must be between 0 and 30 "
            f"(got {s.discovery_request_delay_s})"
        )

    if len(s.discovery_user_agent) > 200:
        errors.append("DISCOVERY_USER_AGENT too long (max 200)")
    if len(s.wikidata_user_agent) > 200:
        errors.append("WIKIDATA_USER_AGENT too long (max 200)")

    # Notion requirements only when enabled
    if s.sync_to_notion == 1:
        if not (s.notion_token or "").strip():
            errors.append("NOTION_TOKEN is required when SYNC_TO_NOTION=1")
        if not (s.notion_data_source_id or "").strip():
            errors.append("NOTION_DATA_SOURCE_ID is required when SYNC_TO_NOTION=1")
        if not (s.notion_version or "").strip():
            errors.append("NOTION_VERSION must be a non-empty string")

    if s.enrich_with_llm not in (0, 1):
        errors.append(f"ENRICH_WITH_LLM must be 0 or 1 (got {s.enrich_with_llm})")

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

        cv_path = s.profile_cv_path
        if cv_path is None:
            errors.append("PROFILES_DIR is set but profile_cv_path resolved to None")
        else:
            cv_file = Path(cv_path)
            if not cv_file.exists():
                errors.append(f"CV file does not exist: {cv_path}")
            elif not cv_file.is_file():
                errors.append(f"CV path is not a file: {cv_path}")

    if errors:
        raise RuntimeError("Invalid configuration: " + "; ".join(errors))


def get_settings() -> Settings:
    load_dotenv()

    def req(name: str) -> str:
        v = os.getenv(name)
        if v is None or not v.strip():
            raise RuntimeError(f"Missing required env var: {name}")
        return v.strip()

    # Parse toggles early so we can conditionally require Notion settings
    sync_to_notion_raw = os.getenv("SYNC_TO_NOTION", "1")
    try:
        sync_to_notion = int(sync_to_notion_raw)
    except ValueError as exc:
        raise RuntimeError(
            f"SYNC_TO_NOTION must be an integer (got {sync_to_notion_raw!r})"
        ) from exc

    enrich_with_llm_raw = os.getenv("ENRICH_WITH_LLM", "0")
    try:
        enrich_with_llm = int(enrich_with_llm_raw)
    except ValueError as exc:
        raise RuntimeError(
            f"ENRICH_WITH_LLM must be an integer (got {enrich_with_llm_raw!r})"
        ) from exc

    if sync_to_notion == 1:
        notion_token = req("NOTION_TOKEN")
        notion_data_source_id = req("NOTION_DATA_SOURCE_ID")
    else:
        notion_token = (os.getenv("NOTION_TOKEN") or "").strip()
        notion_data_source_id = (os.getenv("NOTION_DATA_SOURCE_ID") or "").strip()

    discovery_enable_raw = os.getenv("DISCOVERY_ENABLE", "0")
    try:
        discovery_enable = int(discovery_enable_raw)
    except ValueError as exc:
        raise RuntimeError(
            f"DISCOVERY_ENABLE must be an integer (got {discovery_enable_raw!r})"
        ) from exc

    discovery_verify_enable_raw = os.getenv("DISCOVERY_VERIFY_ENABLE", "0")
    try:
        discovery_verify_enable = int(discovery_verify_enable_raw)
    except ValueError as exc:
        raise RuntimeError(
            "DISCOVERY_VERIFY_ENABLE must be an integer "
            f"(got {discovery_verify_enable_raw!r})"
        ) from exc

    settings = Settings(
        notion_token=notion_token,
        notion_version=(os.getenv("NOTION_VERSION", "2025-09-03") or "").strip(),
        notion_data_source_id=notion_data_source_id,
        mysql_host=req("MYSQL_HOST"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_db=req("MYSQL_DB"),
        mysql_user=req("MYSQL_USER"),
        mysql_password=req("MYSQL_PASSWORD"),
        sync_limit=int(os.getenv("SYNC_LIMIT", "50")),
        fit_min=int(os.getenv("FIT_MIN", "60")),
        max_calls_per_day=int(os.getenv("MAX_CALLS_PER_DAY", "50")),
        request_timeout_s=int(os.getenv("REQUEST_TIMEOUT_S", "20")),
        greenhouse_per_page=int(os.getenv("GREENHOUSE_PER_PAGE", "100")),
        greenhouse_max_pages=int(os.getenv("GREENHOUSE_MAX_PAGES", "50")),
        ingest_per_source_limit=int(os.getenv("INGEST_PER_SOURCE_LIMIT", "0")),
        max_fetch_per_run=int(os.getenv("MAX_FETCH_PER_RUN", "50")),
        max_new_jobs_per_day=int(os.getenv("MAX_NEW_JOBS_PER_DAY", "200")),
        sync_to_notion=sync_to_notion,
        enrich_with_llm=enrich_with_llm,
        enrich_limit=int(os.getenv("ENRICH_LIMIT", "10")),
        openai_api_key=(os.getenv("OPENAI_API_KEY") or "").strip() or None,
        openai_model=(os.getenv("OPENAI_MODEL", "gpt-4.1-mini") or "").strip(),
        openai_base_url=(
            os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1") or ""
        ).strip(),
        profile_id=(os.getenv("PROFILE_ID", "default") or "default").strip(),
        profiles_dir=(os.getenv("PROFILES_DIR", "") or "").strip(),
        # Source discovery
        discovery_enable=discovery_enable,
        discovery_regions=(
            os.getenv("DISCOVERY_REGIONS", "US,EU,CH,CANADA,UAE") or ""
        ).strip(),
        discovery_max_companies_per_region=int(
            os.getenv("DISCOVERY_MAX_COMPANIES_PER_REGION", "5000")
        ),
        discovery_request_delay_s=float(os.getenv("DISCOVERY_REQUEST_DELAY_S", "1.0")),
        discovery_max_sources_per_run=int(
            os.getenv("DISCOVERY_MAX_SOURCES_PER_RUN", "500")
        ),
        discovery_verify_enable=discovery_verify_enable,
        discovery_verify_max_per_run=int(
            os.getenv("DISCOVERY_VERIFY_MAX_PER_RUN", "200")
        ),
        discovery_user_agent=(
            os.getenv("DISCOVERY_USER_AGENT", "jobs-bot/1.0 (+https://example.invalid)")
            or ""
        ).strip(),
        wikidata_user_agent=(
            os.getenv("WIKIDATA_USER_AGENT", "jobs-bot/1.0 (+https://example.invalid)")
            or ""
        ).strip(),
        wikidata_name_fallback_enable=int(
            os.getenv("WIKIDATA_NAME_FALLBACK_ENABLE", "0")
        ),
    )

    validate_settings(settings)
    return settings
