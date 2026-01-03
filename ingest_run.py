from __future__ import annotations

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.notion_client import NotionClient
from jobs_bot.sync_notion import sync_pending_jobs


def main() -> None:
    settings = get_settings()
    SessionLocal = make_session_factory(settings)

    with SessionLocal() as session:
        sources_ok, jobs_created = ingest_all_sources(
            session,
            max_calls_per_day=settings.max_calls_per_day,
            max_new_jobs_per_day=settings.max_new_jobs_per_day,
            max_fetch_per_run=settings.max_fetch_per_run,
            request_timeout_s=settings.request_timeout_s,
            greenhouse_per_page=settings.greenhouse_per_page,
            greenhouse_max_pages=settings.greenhouse_max_pages,
            per_source_limit=(settings.ingest_per_source_limit or None),
        )
        print(f"Sources OK: {sources_ok}, Jobs created: {jobs_created}")

        if settings.sync_to_notion:
            notion = NotionClient(
                token=settings.notion_token,
                version=settings.notion_version,
                data_source_id=settings.notion_data_source_id,
                timeout_s=settings.request_timeout_s,
            )
            n = sync_pending_jobs(
                session,
                notion=notion,
                limit=settings.sync_limit,
                fit_min=settings.fit_min,
            )
            print(f"Synced {n} job(s) to Notion.")


if __name__ == "__main__":
    main()
