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
    Returns True only if a NEW row was created.
    Updates existing rows regardless of daily new cap.
    """
    ats_job_id = str(payload["ats_job_id"])
    job_uid = sha1_uid(src.ats_type, src.company_slug, ats_job_id)

    company = (src.company_name or src.company_slug or "").strip()[:255] or src.company_slug[:255]

    job = session.get(Job, job_uid)
    if job is None:
        # hard stop on NEW inserts only
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

    # Update fields (both new and existing)
    job.title = (payload.get("title") or "Untitled")[:512]
    job.company = company
    job.url = (payload.get("url") or "")[:1024]

    loc = payload.get("location_raw") or None
    job.location_raw = loc[:512] if isinstance(loc, str) and loc else None

    wp = payload.get("workplace_raw") or None
    job.workplace_raw = wp[:128] if isinstance(wp, str) and wp else None

    job.posted_at = payload.get("posted_at")

    job.raw_json = payload.get("raw_json") or {}
    raw_text = payload.get("raw_text") or None
    job.raw_text = raw_text  # longtext ok

    sal = payload.get("salary_text") or None
    job.salary_text = sal[:255] if isinstance(sal, str) and sal else None

    return created


def ingest_all_sources(
    session: Session,
    *,
    max_calls_per_day: int,
    max_fetch_per_run: int,
    max_new_jobs_per_day: int,
    request_timeout_s: int,
    greenhouse_per_page: int,
    greenhouse_max_pages: int,
    per_source_limit: int | None = None,
) -> tuple[int, int]:
    """
    Returns (sources_ok, jobs_created).
    - sources_ok: count of sources completed with src.last_error == None
    - jobs_created: number of NEW jobs inserted (not updates)
    """
    now = utcnow_naive()
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()

    sources_ok = 0
    jobs_created_total = 0
    jobs_processed = 0  # run-level cap

    def reached_run_cap() -> bool:
        return max_fetch_per_run > 0 and jobs_processed >= max_fetch_per_run

    for src in sources:
        created_this_source = 0
        partial_error: str | None = None

        try:
            if src.ats_type == "lever":
                if reached_run_cap():
                    session.commit()
                    return sources_ok, jobs_created_total

                if not can_consume_call(session, "lever", max_calls_per_day):
                    src.last_error = "rate limit reached (lever)"
                    session.commit()
                    continue

                # IMPORTANT: persist the usage BEFORE HTTP call (otherwise rollback removes the charge)
                session.commit()

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)
                if per_source_limit:
                    postings = postings[:per_source_limit]

                for p in postings:
                    if reached_run_cap():
                        # commit what we have and exit the whole run
                        src.last_ok_at = now
                        src.last_error = partial_error
                        session.commit()
                        return sources_ok, jobs_created_total

                    created = upsert_job(
                        session,
                        src,
                        p,
                        now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        created_this_source += 1

            elif src.ats_type == "greenhouse":
                all_jobs: list[dict] = []

                for page in range(1, greenhouse_max_pages + 1):
                    if reached_run_cap():
                        break

                    if not can_consume_call(session, "greenhouse", max_calls_per_day):
                        partial_error = "rate limit reached during pagination (greenhouse)"
                        break

                    # charge the call before HTTP
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

                    if per_source_limit and len(all_jobs) >= per_source_limit:
                        all_jobs = all_jobs[:per_source_limit]
                        break

                if not all_jobs:
                    # nothing fetched; keep last_error if set, otherwise mark as noop
                    if partial_error:
                        src.last_error = partial_error
                    session.commit()
                    continue

                for j in all_jobs:
                    if reached_run_cap():
                        break

                    created = upsert_job(
                        session,
                        src,
                        j,
                        now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        created_this_source += 1

            else:
                src.last_error = f"unsupported ats_type={src.ats_type}"
                session.commit()
                continue

            # success commit for this source (even if partial_error not None)
            src.last_ok_at = now
            src.last_error = partial_error
            session.commit()

            if partial_error is None:
                sources_ok += 1
            jobs_created_total += created_this_source

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
