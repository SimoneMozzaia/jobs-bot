from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .api_usage import utcnow_naive
from .llm_client import LlmClientError, LlmEnrichment, OpenAIResponsesClient
from .models import Job, JobEnrichment


logger = logging.getLogger("jobs-bot")


@dataclass(frozen=True)
class EnrichStats:
    attempted: int
    enriched: int
    failed: int


def _join_lines(items: list[str]) -> str | None:
    cleaned = [x.strip() for x in items if x and x.strip()]
    if not cleaned:
        return None
    return "\n".join(cleaned)


def _is_outdated(job: Job, enr: JobEnrichment | None) -> bool:
    if enr is None:
        return True
    if enr.enriched_at is None:
        return True
    return job.last_checked > enr.enriched_at


def enrich_pending_jobs(
    session: Session,
    *,
    client: OpenAIResponsesClient,
    limit: int,
) -> EnrichStats:
    """
    Enrich jobs into job_enrichment.

    A job is eligible when:
      - no job_enrichment row exists, OR
      - job.last_checked > job_enrichment.enriched_at (stale enrichment)

    Returns stats: attempted/enriched/failed.
    """
    now = utcnow_naive()

    stmt = (
        select(Job, JobEnrichment)
        .outerjoin(JobEnrichment, JobEnrichment.job_uid == Job.job_uid)
        .where(
            or_(
                JobEnrichment.job_uid.is_(None),
                JobEnrichment.enriched_at.is_(None),
                Job.last_checked > JobEnrichment.enriched_at,
            )
        )
        .order_by(Job.last_checked.desc())
        .limit(limit)
    )

    rows = session.execute(stmt).all()

    attempted = len(rows)
    enriched = 0
    failed = 0

    for job, enr in rows:
        if not _is_outdated(job, enr):
            continue

        try:
            out: LlmEnrichment = client.enrich_job(
                title=job.title,
                company=job.company,
                location_raw=job.location_raw,
                workplace_raw=job.workplace_raw,
                url=job.url,
                salary_text=job.salary_text,
                raw_text=job.raw_text,
            )

            if enr is None:
                enr = JobEnrichment(job_uid=job.job_uid)
                session.add(enr)

            enr.summary = out.summary or None
            enr.skills_json = out.skills  # JSON column; list is OK
            enr.pros = _join_lines(out.pros)
            enr.cons = _join_lines(out.cons)
            enr.outreach_target = out.outreach_target or None
            enr.salary = out.salary
            enr.llm_model = out.model
            enr.llm_tokens = out.total_tokens
            enr.enriched_at = now

            session.commit()
            enriched += 1

            logger.info(
                "Job enriched.",
                extra={
                    "event": "llm_enrich_ok",
                    "job_uid": job.job_uid,
                    "llm_model": out.model,
                    "llm_tokens": out.total_tokens,
                },
            )

        except LlmClientError as e:
            session.rollback()
            failed += 1
            logger.error(
                "LLM enrichment failed.",
                extra={"event": "llm_enrich_failed", "job_uid": job.job_uid, "error": str(e)},
            )
        except Exception:
            session.rollback()
            failed += 1
            logger.exception(
                "Unexpected enrichment error.",
                extra={"event": "llm_enrich_failed_unexpected", "job_uid": job.job_uid},
            )

    return EnrichStats(attempted=attempted, enriched=enriched, failed=failed)
