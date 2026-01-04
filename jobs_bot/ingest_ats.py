from __future__ import annotations

import datetime as dt
import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from .api_usage import can_consume_call, can_create_new_job, utcnow_naive
from .ats_clients import fetch_greenhouse_jobs_page, fetch_lever_postings
from .models import Job, Source


def sha1_uid(ats_type: str, company_slug: str, ats_job_id: str) -> str:
    raw = f"{ats_type}:{company_slug}:{ats_job_id}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def upsert_job(
    session: Session,
    *,
    src: Source,
    payload: dict,
    now: dt.datetime,
    max_new_jobs_per_day: int,
) -> bool:
    """
    Upsert a job into DB.

    Returns:
      True  -> newly created row
      False -> updated existing row OR skipped due to daily new-job cap
    """
    ats_job_id = str(payload["ats_job_id"])
    job_uid = sha1_uid(src.ats_type, src.company_slug, ats_job_id)

    company = (src.company_name or src.company_slug or "").strip()[:255] or src.company_slug[:255]

    job = session.get(Job, job_uid)
    if job is None:
        if not can_create_new_job(session, max_new_jobs_per_day):
            return False

        job = Job(
            job_uid=job_uid,
            source_id=src.id,
            ats_job_id=ats_job_id,
            first_seen=now,
            last_seen=now,
            last_checked=now,
            fit_score=0,
            fit_class="No",
            raw_json=payload.get("raw_json") or {},
        )
        session.add(job)
        created = True
    else:
        job.last_seen = now
        job.last_checked = now
        created = False

    job.title = (payload.get("title") or "Untitled")[:512]
    job.company = company
    job.url = (payload.get("url") or "")[:1024]

    location_raw = payload.get("location_raw") or None
    job.location_raw = location_raw[:512] if isinstance(location_raw, str) and location_raw else None

    workplace_raw = payload.get("workplace_raw") or None
    job.workplace_raw = workplace_raw[:128] if isinstance(workplace_raw, str) and workplace_raw else None

    job.posted_at = payload.get("posted_at")
    job.raw_json = payload.get("raw_json") or {}
    job.raw_text = payload.get("raw_text") or None

    salary_text = payload.get("salary_text") or None
    job.salary_text = salary_text[:255] if isinstance(salary_text, str) and salary_text else None

    return created


def _finalize_source_ok(session: Session, src: Source, now: dt.datetime) -> None:
    src.last_ok_at = now
    src.last_error = None
    session.commit()


def ingest_all_sources(
    session: Session,
    *,
    max_calls_per_day: int,
    max_new_jobs_per_day: int,
    max_fetch_per_run: int,
    request_timeout_s: int,
    greenhouse_per_page: int,
    greenhouse_max_pages: int,
    per_source_limit: int | None = None,
) -> tuple[int, int]:
    """
    Ingest jobs from all active sources.

    Returns:
      (sources_ok, jobs_created)

    Semantics:
    - sources_ok increments only when a source ingestion completes successfully
      (even if we stop early due to max_fetch_per_run).
    - jobs_created counts only NEW Job rows (updates do not increment).
    """
    now = utcnow_naive()
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()

    sources_ok = 0
    created_total = 0
    jobs_processed = 0

    for src in sources:
        partial_error: str | None = None

        try:
            if src.ats_type == "lever":
                if not can_consume_call(session, "lever", max_calls_per_day):
                    src.last_error = "rate limit reached (lever)"
                    session.commit()
                    continue

                # Persist call budget consumption strictly.
                session.commit()

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)
                if per_source_limit:
                    postings = postings[:per_source_limit]

                for p in postings:
                    if max_fetch_per_run and jobs_processed >= max_fetch_per_run:
                        # Stop the run gracefully: current source is still OK.
                        _finalize_source_ok(session, src, now)
                        sources_ok += 1
                        return sources_ok, created_total

                    created = upsert_job(
                        session,
                        src=src,
                        payload=p,
                        now=now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        created_total += 1

                _finalize_source_ok(session, src, now)
                sources_ok += 1
                continue

            if src.ats_type == "greenhouse":
                all_jobs: list[dict] = []

                for page in range(1, greenhouse_max_pages + 1):
                    if not can_consume_call(session, "greenhouse", max_calls_per_day):
                        partial_error = "rate limit reached during pagination (greenhouse)"
                        break

                    # Persist call budget consumption strictly (1 page == 1 call).
                    session.commit()

                    page_jobs = fetch_greenhouse_jobs_page(
                        src.api_base,
                        page=page,
                        timeout_s=request_timeout_s,
                        per_page=greenhouse_per_page,
                    )
                    if not page_jobs:
                        break

                    all_jobs.extend(page_jobs)

                    if max_fetch_per_run and jobs_processed >= max_fetch_per_run:
                        # We reached the global processing cap while paging.
                        break

                if not all_jobs:
                    src.last_error = partial_error
                    session.commit()
                    continue

                if per_source_limit:
                    all_jobs = all_jobs[:per_source_limit]

                for j in all_jobs:
                    if max_fetch_per_run and jobs_processed >= max_fetch_per_run:
                        # Stop the run gracefully: current source is still OK if we ingested any jobs.
                        _finalize_source_ok(session, src, now)
                        sources_ok += 1
                        return sources_ok, created_total

                    created = upsert_job(
                        session,
                        src=src,
                        payload=j,
                        now=now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        created_total += 1

                # If we had a pagination partial error, record it; otherwise mark clean success.
                src.last_ok_at = now
                src.last_error = partial_error
                session.commit()

                if partial_error is None:
                    sources_ok += 1
                continue

            # Unsupported provider
            src.last_error = f"unsupported ats_type={src.ats_type}"
            session.commit()

        except Exception as exc:  # noqa: BLE001
            # Do not keep partial job inserts if anything failed mid-source.
            session.rollback()
            try:
                src.last_error = f"{type(exc).__name__}: {exc}"
                session.commit()
            except Exception:  # noqa: BLE001
                session.rollback()

    return sources_ok, created_total
