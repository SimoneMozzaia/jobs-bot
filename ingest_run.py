from __future__ import annotations

import sys
import uuid

from sqlalchemy import select

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.logging_utils import LogContext, configure_logging
from jobs_bot.models import Source
from jobs_bot.notion_client import NotionClient
from jobs_bot.sync_notion import sync_pending_jobs


def _collect_active_source_errors(session) -> list[str]:
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()
    errors: list[str] = []
    for src in sources:
        if src.last_error:
            errors.append(f"{src.ats_type}:{src.company_slug}: {src.last_error}")
    return errors


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    logger = configure_logging(context=LogContext(run_id=run_id))

    try:
        settings = get_settings()
    except Exception:
        logger.exception("Configuration invalid.", extra={"event": "config_invalid"})
        sys.exit(2)

    logger.info(
        "Starting ingest run.",
        extra={
            "event": "ingest_start",
            "max_calls_per_day": settings.max_calls_per_day,
            "max_new_jobs_per_day": settings.max_new_jobs_per_day,
            "max_fetch_per_run": settings.max_fetch_per_run,
            "sync_to_notion": settings.sync_to_notion,
        },
    )

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

        logger.info(
            "Ingest completed.",
            extra={
                "event": "ingest_done",
                "sources_ok": sources_ok,
                "jobs_created": jobs_created,
            },
        )

        # Minimal alerting: if nothing succeeded, distinguish rate-limit vs actual failure.
        if sources_ok == 0:
            errors = _collect_active_source_errors(session)
            lower = [e.lower() for e in errors]
            all_rate_limited = bool(errors) and all("rate limit" in e for e in lower)

            if all_rate_limited:
                logger.warning(
                    "All sources blocked by rate limit.",
                    extra={"event": "alert_rate_limited", "errors": errors},
                )
            else:
                logger.error(
                    "No sources succeeded during ingest.",
                    extra={"event": "alert_sources_failed", "errors": errors},
                )
                sys.exit(2)

        # Optional Notion sync
        if settings.sync_to_notion:
            try:
                notion = NotionClient(
                    token=settings.notion_token,
                    version=settings.notion_version,
                    data_source_id=settings.notion_data_source_id,
                    timeout_s=settings.request_timeout_s,
                )
                synced = sync_pending_jobs(
                    session,
                    notion=notion,
                    limit=settings.sync_limit,
                    fit_min=settings.fit_min,
                )
                logger.info(
                    "Notion sync completed.",
                    extra={"event": "notion_sync_done", "synced": synced},
                )
            except Exception:
                # Keep ingestion healthy even if Notion is down.
                logger.exception("Notion sync failed.", extra={"event": "notion_sync_failed"})


if __name__ == "__main__":
    main()
