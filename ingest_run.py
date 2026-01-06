from __future__ import annotations

import sys
import uuid

from sqlalchemy.orm import Session

from jobs_bot.config import Settings, get_settings, validate_settings
from jobs_bot.db import make_session_factory
from jobs_bot.enrich_llm import enrich_pending_jobs
from jobs_bot.fit_scoring import compute_fit_scores_for_profile
from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.llm_client import OpenAIResponsesClient
from jobs_bot.logging_utils import LogContext, configure_logging
from jobs_bot.notion_client import NotionClient
from jobs_bot.profile_bootstrap import ProfileBootstrapError, bootstrap_profile
from jobs_bot.sync_notion import sync_pending_jobs


def _extract_enriched_count(stats: object) -> int:
    """
    Backward/forward-compatible adapter for enrich_pending_jobs() return types.

    Historical tests and older implementations may return:
      - int (number of enriched jobs)

    Newer implementations return an object with:
      - .enriched attribute

    This adapter supports both to avoid brittle coupling.
    """
    if isinstance(stats, int):
        return int(stats)

    enriched = getattr(stats, "enriched", None)
    if enriched is not None:
        return int(enriched)

    try:
        return int(stats)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        raise TypeError(
            "Unsupported return type from enrich_pending_jobs(): "
            f"{type(stats).__name__}. Expected int or object with '.enriched'."
        ) from exc


def run_pipeline(session: Session, *, settings: Settings, logger) -> dict[str, int]:
    # Fail fast on configuration inconsistencies.
    #
    # Notion sync is profile-scoped (job_profile), therefore we require
    # multi-profile mode to be enabled via PROFILES_DIR.
    if settings.sync_to_notion and not settings.profiles_dir:
        raise RuntimeError(
            "SYNC_TO_NOTION=1 requires PROFILES_DIR (profile-scoped Notion sync). "
            "Either set PROFILES_DIR (and ensure {PROFILES_DIR}/{PROFILE_ID}/cv.docx exists) "
            "or set SYNC_TO_NOTION=0 to run without Notion."
        )

    results: dict[str, int] = {
        "sources_ok": 0,
        "jobs_created": 0,
        "jobs_enriched": 0,
        "jobs_scored": 0,
        "notion_synced": 0,
    }

    logger.info("ingest_start", extra={"event": "ingest_start"})

    ok, created = ingest_all_sources(
        session,
        max_calls_per_day=settings.max_calls_per_day,
        max_new_jobs_per_day=settings.max_new_jobs_per_day,
        max_fetch_per_run=settings.max_fetch_per_run,
        request_timeout_s=settings.request_timeout_s,
        greenhouse_per_page=settings.greenhouse_per_page,
        greenhouse_max_pages=settings.greenhouse_max_pages,
        per_source_limit=(settings.ingest_per_source_limit or None),
    )
    results["sources_ok"] = int(ok)
    results["jobs_created"] = int(created)

    if settings.enrich_with_llm:
        client = OpenAIResponsesClient(
            api_key=settings.openai_api_key or "",
            model=settings.openai_model,
            base_url=settings.openai_base_url,
            timeout_s=settings.request_timeout_s,
        )
        stats = enrich_pending_jobs(session, client=client, limit=settings.enrich_limit)
        results["jobs_enriched"] = _extract_enriched_count(stats)

    if settings.profiles_dir:
        cv_path = settings.profile_cv_path
        if not cv_path:
            raise ProfileBootstrapError(
                "PROFILE_ID/PROFILES_DIR produced an empty cv path"
            )

        profile, changed = bootstrap_profile(
            session,
            profile_id=settings.profile_id,
            cv_path=cv_path,
        )
        logger.info(
            "profile_bootstrap_done",
            extra={
                "event": "profile_bootstrap_done",
                "profile_id": profile.profile_id,
                "changed": bool(changed),
            },
        )

        stats = compute_fit_scores_for_profile(
            session,
            profile=profile,
            limit=settings.sync_limit,
        )
        results["jobs_scored"] = int(stats.attempted)

    if settings.sync_to_notion:
        notion = NotionClient(
            token=settings.notion_token,
            version=settings.notion_version,
            data_source_id=settings.notion_data_source_id,
        )
        synced = sync_pending_jobs(
            session,
            notion=notion,
            limit=settings.sync_limit,
            fit_min=settings.fit_min,
            profile_id=settings.profile_id,
        )
        results["notion_synced"] = int(synced)

    logger.info("ingest_done", extra={"event": "ingest_done", **results})
    return results


def main() -> int:
    run_id = str(uuid.uuid4())
    logger = configure_logging(context=LogContext(run_id=run_id))

    try:
        settings = get_settings()
        validate_settings(settings)
    except Exception:
        logger.exception("invalid_settings", extra={"event": "invalid_settings"})
        return 2

    SessionLocal = make_session_factory(settings)

    try:
        with SessionLocal() as session:
            run_pipeline(session, settings=settings, logger=logger)
        return 0
    except Exception:
        logger.exception("run_failed", extra={"event": "run_failed"})
        return 1


if __name__ == "__main__":
    sys.exit(main())
