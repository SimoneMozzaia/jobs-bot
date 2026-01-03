from __future__ import annotations

import datetime as dt
import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from .api_usage import can_consume_call, can_create_new_job, utcnow_naive
from .ats_clients import fetch_greenhouse_jobs_page, fetch_lever_postings
from .models import Job, Source


def sha1_uid(ats_type: str, company_slug: str, ats_job_id: str) -> str:
    s = f"{ats_type}:{company_slug}:{ats_job_id}".encode("utf-8")
    return hashlib.sha1(s).hexdigest()


def upsert_job(
    session: Session,
    src: Source,
    payload: dict,
    now: dt.datetime,
    *,
    max_new_jobs_per_day: int,
) -> bool:
    """
    Upsert a single job into DB.
    Returns True if a new row was created, False if it was an update or skipped.
    """
    ats_job_id = str(payload["ats_job_id"])
    job_uid = sha1_uid(src.ats_type, src.company_slug, ats_job_id)

    company = (src.company_name or src.company_slug or "").strip()[:255] or src.company_slug[:255]

    job = session.get(Job, job_uid)
    created = False

    if not job:
        if not can_create_new_job(session, max_new_jobs_per_day):
            return False  # hard skip new inserts once daily cap is hit

        job = Job(
            job_uid=job_uid,
            source_id=src.id,
            ats_job_id=ats_job_id,
            title=(payload.get("title") or "Untitled")[:512],
            company=company,
            url=(payload.get("url") or "")[:1024],
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
        job.title = (payload.get("title") or job.title or "Untitled")[:512]
        job.company = company
        job.url = (payload.get("url") or job.url or "")[:1024]
        job.raw_json = payload.get("raw_json") or job.raw_json or {}

    job.location_raw = payload.get("location_raw") or None
    if job.location_raw:
        job.location_raw = job.location_raw[:512]

    job.workplace_raw = payload.get("workplace_raw") or None
    if job.workplace_raw:
        job.workplace_raw = job.workplace_raw[:128]

    job.posted_at = payload.get("posted_at")

    raw_text = payload.get("raw_text") or None
    job.raw_text = raw_text

    job.salary_text = payload.get("salary_text") or None
    if job.salary_text:
        job.salary_text = job.salary_text[:255]

    return created


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

    Notes:
      - Provider call usage is counted per HTTP call (Lever = 1, Greenhouse = 1/page).
      - New job creation is capped globally per day.
      - The run stops early when `max_fetch_per_run` is hit (0/None means unlimited).
    """
    now = utcnow_naive()
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()

    sources_ok = 0
    jobs_created_total = 0
    jobs_processed = 0

    def reached_run_cap() -> bool:
        return bool(max_fetch_per_run and jobs_processed >= max_fetch_per_run)

    for src in sources:
        src_jobs_created = 0
        partial_error: str | None = None

        try:
            if src.ats_type == "lever":
                session.begin()
                if not can_consume_call(session, "lever", max_calls_per_day):
                    src.last_error = "rate limit reached (lever)"
                    session.commit()
                    continue

                session.commit()

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)
                if per_source_limit:
                    postings = postings[:per_source_limit]

                for p in postings:
                    created = upsert_job(
                        session,
                        src,
                        p,
                        now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        src_jobs_created += 1
                    if reached_run_cap():
                        break

            elif src.ats_type == "greenhouse":
                all_jobs: list[dict] = []

                for page in range(1, greenhouse_max_pages + 1):
                    session.begin()
                    if not can_consume_call(session, "greenhouse", max_calls_per_day):
                        partial_error = "rate limit reached during pagination (greenhouse)"
                        session.commit()
                        break
                    session.commit()

                    page_jobs = fetch_greenhouse_jobs_page(
                        src.api_base,
                        page=page,
                        timeout_s=request_timeout_s,
                        per_page=greenhouse_per_page,
                    )
                    if not page_jobs:
                        break

                    if per_source_limit:
                        remaining = max(per_source_limit - len(all_jobs), 0)
                        page_jobs = page_jobs[:remaining]

                    all_jobs.extend(page_jobs)

                    for j in page_jobs:
                        created = upsert_job(
                            session,
                            src,
                            j,
                            now,
                            max_new_jobs_per_day=max_new_jobs_per_day,
                        )
                        jobs_processed += 1
                        if created:
                            src_jobs_created += 1
                        if reached_run_cap():
                            break

                    if reached_run_cap() or (per_source_limit and len(all_jobs) >= per_source_limit):
                        break

                if not all_jobs and partial_error:
                    src.last_error = partial_error
                    session.commit()
                    continue

            else:
                src.last_error = f"unsupported ats_type={src.ats_type}"
                session.commit()
                continue

            src.last_ok_at = now
            src.last_error = partial_error
            session.commit()

            sources_ok += 1
            jobs_created_total += src_jobs_created

            if reached_run_cap():
                return sources_ok, jobs_created_total

        except Exception as e:
            session.rollback()
            try:
                src.last_error = f"{type(e).__name__}: {e}"
                session.commit()
            except Exception:
                session.rollback()

    return sources_ok, jobs_created_total
