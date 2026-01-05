from __future__ import annotations

import datetime as dt
import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from .api_usage import can_consume_call, can_create_new_job, utcnow_naive
from .ats_clients import (
    fetch_greenhouse_job_detail,
    fetch_greenhouse_jobs_page,
    fetch_lever_postings,
)
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
    ats_job_id = payload["ats_job_id"]
    job_uid = sha1_uid(src.ats_type, src.company_slug, ats_job_id)

    company = (src.company_name or src.company_slug or "").strip()[:255] or src.company_slug[:255]

    job = session.get(Job, job_uid)
    if not job:
        if not can_create_new_job(session, max_new_jobs_per_day):
            return False
        job = Job(
            job_uid=job_uid,
            source_id=src.id,
            ats_job_id=str(ats_job_id),
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

    job.location_raw = payload.get("location_raw") or None
    if job.location_raw:
        job.location_raw = job.location_raw[:512]

    job.workplace_raw = payload.get("workplace_raw") or None
    if job.workplace_raw:
        job.workplace_raw = job.workplace_raw[:128]

    job.posted_at = payload.get("posted_at")
    job.raw_json = payload.get("raw_json") or {}
    job.raw_text = payload.get("raw_text") or None

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
    now = utcnow_naive()
    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()

    sources_ok = 0
    jobs_created = 0
    jobs_processed = 0

    for src in sources:
        # Global per-run cap: stop before spending any additional API calls.
        if jobs_processed >= max_fetch_per_run:
            return sources_ok, jobs_created

        try:
            if src.ats_type == "lever":
                if not can_consume_call(session, "lever", max_calls_per_day):
                    src.last_error = "rate limit reached (lever)"
                    session.commit()
                    continue

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)
                if per_source_limit:
                    postings = postings[:per_source_limit]

                for p in postings:
                    if jobs_processed >= max_fetch_per_run:
                        # Source succeeded but run stops early due to per-run cap.
                        src.last_ok_at = now
                        src.last_error = None
                        session.commit()
                        sources_ok += 1
                        return sources_ok, jobs_created

                    created = upsert_job(
                        session,
                        src,
                        p,
                        now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        jobs_created += 1

            elif src.ats_type == "greenhouse":
                all_jobs: list[dict] = []
                stop_detail_fetch = False

                for page in range(1, greenhouse_max_pages + 1):
                    if not can_consume_call(session, "greenhouse", max_calls_per_day):
                        src.last_error = "rate limit reached during pagination (greenhouse)"
                        break

                    page_jobs = fetch_greenhouse_jobs_page(
                        src.api_base,
                        page=page,
                        timeout_s=request_timeout_s,
                        per_page=greenhouse_per_page,
                    )
                    if not page_jobs:
                        break

                    all_jobs.extend(page_jobs)

                if not all_jobs:
                    session.commit()
                    continue

                if per_source_limit:
                    all_jobs = all_jobs[:per_source_limit]

                for j in all_jobs:
                    if jobs_processed >= max_fetch_per_run:
                        src.last_ok_at = now
                        src.last_error = None
                        session.commit()
                        sources_ok += 1
                        return sources_ok, jobs_created

                    # Optional detail call (consumes call budget) only when list payload lacks text.
                    if not j.get("raw_text"):
                        if stop_detail_fetch:
                            pass
                        elif not can_consume_call(session, "greenhouse", max_calls_per_day):
                            stop_detail_fetch = True
                        else:
                            try:
                                detail = fetch_greenhouse_job_detail(
                                    src.api_base,
                                    ats_job_id=str(j.get("ats_job_id")),
                                    timeout_s=request_timeout_s,
                                )
                                j.update(detail)
                            except Exception:
                                # Keep ingestion robust: detail failure shouldn't block list ingestion.
                                pass

                    created = upsert_job(
                        session,
                        src,
                        j,
                        now,
                        max_new_jobs_per_day=max_new_jobs_per_day,
                    )
                    jobs_processed += 1
                    if created:
                        jobs_created += 1

            else:
                src.last_error = f"unsupported ats_type={src.ats_type}"
                session.commit()
                continue

            src.last_ok_at = now
            src.last_error = None
            session.commit()
            sources_ok += 1

        except Exception as exc:
            session.rollback()
            try:
                src.last_error = f"{type(exc).__name__}: {exc}"
                session.commit()
            except Exception:
                session.rollback()

    return sources_ok, jobs_created
