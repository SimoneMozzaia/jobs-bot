from __future__ import annotations

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.notion_client import NotionClient
from jobs_bot.sync_notion import sync_pending_jobs


def main():
    s = get_settings()
    SessionLocal = make_session_factory(s)

    with SessionLocal() as session:
        ok, created = ingest_all_sources(
            session,
            max_calls_per_day=s.max_calls_per_day,
            max_fetch_per_run=s.max_fetch_per_run,
            max_new_jobs_per_day=s.max_new_jobs_per_day,
            request_timeout_s=s.request_timeout_s,
            greenhouse_per_page=s.greenhouse_per_page,
            greenhouse_max_pages=s.greenhouse_max_pages,
            per_source_limit=(s.ingest_per_source_limit or None),
        )
        print(f"Sources OK: {ok}, Jobs created: {created}")

        if s.sync_to_notion == 1:
            notion = NotionClient(
                token=s.notion_token,
                version=s.notion_version,
                data_source_id=s.notion_data_source_id,
            )
            synced = sync_pending_jobs(session, notion, limit=s.sync_limit, fit_min=s.fit_min)
            print(f"Synced {synced} job(s) to Notion.")


if __name__ == "__main__":
    main()
