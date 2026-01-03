from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    # Notion
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

    ingest_per_source_limit: int = 0  # 0 = no limit
    max_fetch_per_run: int = 50
    max_new_jobs_per_day: int = 200
    sync_to_notion: int = 1  # 1/0

    @property
    def mysql_url(self) -> str:
        # charset important for emojis / multilingual
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
            f"?charset=utf8mb4"
        )


def get_settings() -> Settings:
    load_dotenv()

    def req(name: str) -> str:
        v = os.getenv(name)
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v

    return Settings(
        notion_token=req("NOTION_TOKEN"),
        notion_version=os.getenv("NOTION_VERSION", "2025-09-03"),
        notion_data_source_id=req("NOTION_DATA_SOURCE_ID"),

        mysql_host=req("MYSQL_HOST"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_db=req("MYSQL_DB"),
        mysql_user=req("MYSQL_USER"),
        mysql_password=req("MYSQL_PASSWORD"),

        # Runtime
        sync_limit=int(os.getenv("SYNC_LIMIT", str(Settings.sync_limit))),
        fit_min=int(os.getenv("FIT_MIN", str(Settings.fit_min))),
        max_calls_per_day=int(os.getenv("MAX_CALLS_PER_DAY", str(Settings.max_calls_per_day))),

        request_timeout_s=int(os.getenv("REQUEST_TIMEOUT_S", str(Settings.request_timeout_s))),
        greenhouse_per_page=int(os.getenv("GREENHOUSE_PER_PAGE", str(Settings.greenhouse_per_page))),
        greenhouse_max_pages=int(os.getenv("GREENHOUSE_MAX_PAGES", str(Settings.greenhouse_max_pages))),

        ingest_per_source_limit=int(os.getenv("INGEST_PER_SOURCE_LIMIT", str(Settings.ingest_per_source_limit))),
        max_fetch_per_run=int(os.getenv("MAX_FETCH_PER_RUN", "50")),
        max_new_jobs_per_day=int(os.getenv("MAX_NEW_JOBS_PER_DAY", "200")),
        sync_to_notion=int(os.getenv("SYNC_TO_NOTION", "1")),

    )

