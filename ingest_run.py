from __future__ import annotations

import sys
import uuid

from sqlalchemy import select

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.enrich_llm import enrich_pending_jobs
from jobs_bot.fit_scoring import compute_fit_scores_for_profile
from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.llm_client import OpenAIResponsesClient
from jobs_bot.logging_utils import LogContext, configure_logging
from jobs_bot.models import Profile, Source
from jobs_bot.notion_client import NotionClient
from jobs_bot.profile_bootstrap import bootstrap_profile
from jobs_bot.sync_notion import sync_pending_jobs


def _collect_active_source_errors(session) -> list[str]:
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()
    return [
        f"{src.ats_type}:{src.company_slug}: {src.last_error}"
        for src in sources
        if src.last_error
    ]


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
            "enrich_with_llm": settings.enrich_with_llm,
            "enrich_limit": settings.enrich_limit,
            "multi_profile": settings.multi_profile_enabled,
            "profile_id": settings.profile_id,
        },
    )

    SessionLocal = make_session_factory(settings)

    with SessionLocal() as session:
        profile: Profile | None = None

        if settings.multi_profile_enabled and settings.profile_cv_path:
            try:
                profile, changed = bootstrap_profile(
                    session,
                    profile_id=settings.profile_id,
                    cv_path=settings.profile_cv_path,
                )
                logger.info(
                    "Profile bootstrapped.",
                    extra={
                        "event": "profile_bootstrap_done",
                        "profile_id": settings.profile_id,
                        "changed": changed,
                        "cv_sha256": profile.cv_sha256,
                    },
                )
            except Exception:
                logger.exception("Profile bootstrap failed.", extra={"event": "profile_bootstrap_failed"})
                sys.exit(2)

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

        if settings.enrich_with_llm:
            try:
                client = OpenAIResponsesClient(
                    api_key=(settings.openai_api_key or ""),
                    model=settings.openai_model,
                    base_url=settings.openai_base_url,
                    timeout_s=settings.request_timeout_s,
                )
                stats = enrich_pending_jobs(session, client=client, limit=settings.enrich_limit)
                logger.info(
                    "LLM enrichment completed.",
                    extra={
                        "event": "llm_enrich_done",
                        "attempted": stats.attempted,
                        "enriched": stats.enriched,
                        "failed": stats.failed,
                        "openai_model": settings.openai_model,
                    },
                )
                if stats.attempted > 0 and stats.enriched == 0 and stats.failed == stats.attempted:
                    logger.error(
                        "All LLM enrichments failed.",
                        extra={"event": "alert_llm_all_failed", "attempted": stats.attempted},
                    )
                    sys.exit(2)

            except Exception:
                logger.exception("LLM enrichment crashed.", extra={"event": "llm_enrich_crashed"})
                sys.exit(2)

        # Fit scoring per profile
        if settings.multi_profile_enabled and profile is not None:
            try:
                fit_stats = compute_fit_scores_for_profile(
                    session,
                    profile=profile,
                    limit=max(1, settings.sync_limit),
                )
                logger.info(
                    "Fit scoring completed.",
                    extra={
                        "event": "fit_scoring_done",
                        "profile_id": profile.profile_id,
                        "attempted": fit_stats.attempted,
                        "inserted": fit_stats.inserted_jobs,
                        "updated": fit_stats.updated_jobs,
                    },
                )
            except Exception:
                logger.exception("Fit scoring failed.", extra={"event": "fit_scoring_failed"})
                sys.exit(2)

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
                    profile_id=settings.profile_id if settings.multi_profile_enabled else None,
                )
                logger.info(
                    "Notion sync completed.",
                    extra={"event": "notion_sync_done", "synced": synced},
                )
            except Exception:
                logger.exception("Notion sync failed.", extra={"event": "notion_sync_failed"})


if __name__ == "__main__":
    main()
