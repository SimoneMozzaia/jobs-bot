from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .api_usage import can_consume_call, can_create_new_job, utcnow_naive
from .ats_clients import fetch_greenhouse_jobs_page, fetch_lever_postings
from .models import Job, Source


def _build_job_uid(ats_type: str, company_slug: str, ats_job_id: str) -> str:
    key = f"{ats_type}:{company_slug}:{ats_job_id}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def upsert_job(session: Session, *, source: Source, posting: dict[str, Any], now: dt.datetime) -> bool:
    """
    Idempotent upsert into jobs.

    Returns True only when a NEW Job row is created.
    Updates are always allowed, even if the "new jobs per day" cap is reached.
    """
    ats_job_id = (posting.get("ats_job_id") or "").strip()
    if not ats_job_id:
        raise ValueError("posting missing ats_job_id")

    company_slug = (getattr(source, "company_slug", None) or "").strip()
    if not company_slug:
        raise ValueError("source missing company_slug")

    ats_type = (getattr(source, "ats_type", None) or "").strip()
    if not ats_type:
        raise ValueError("source missing ats_type")

    job_uid = _build_job_uid(ats_type, company_slug, ats_job_id)

    job = session.get(Job, job_uid)
    created = False

    title = (posting.get("title") or "").strip() or "Untitled"
    url = (posting.get("url") or "").strip() or None
    raw_json = posting.get("raw_json") or {}
    raw_text = posting.get("raw_text") or ""
    salary_text = (posting.get("salary_text") or "").strip() or None
    location_raw = (posting.get("location_raw") or "").strip() or None
    workplace_raw = (posting.get("workplace_raw") or "").strip() or None

    if job is None:
        # NEW job -> respect daily cap
        if not can_create_new_job(session, max_new_per_day=posting.get("max_new_jobs_per_day", 0) or 0):
            return False

        job = Job(
            job_uid=job_uid,
            source_id=source.id,
            ats_job_id=ats_job_id,
            title=title,
            company=(posting.get("company") or getattr(source, "company_name", "") or "").strip() or None,
            url=url,
            first_seen=now,
            last_seen=now,
            last_checked=now,
            raw_json=raw_json,
            raw_text=raw_text,
            salary_text=salary_text,
            location_raw=location_raw,
            workplace_raw=workplace_raw,
            # keep legacy columns for DB backward-compatibility
            fit_score=0,
            fit_class="No",
        )
        session.add(job)
        created = True
    else:
        # UPDATE existing (always allowed)
        job.title = title
        if posting.get("company") is not None:
            job.company = (posting.get("company") or "").strip() or job.company
        if url:
            job.url = url

        job.last_seen = now
        job.last_checked = now

        job.raw_json = raw_json
        if raw_text is not None:
            job.raw_text = raw_text

        if salary_text is not None:
            job.salary_text = salary_text
        if location_raw is not None:
            job.location_raw = location_raw
        if workplace_raw is not None:
            job.workplace_raw = workplace_raw

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
    per_source_limit: int | None,
) -> tuple[int, int]:
    """
    Ingest postings from all active sources.

    Returns:
      (sources_ok, jobs_created)

    Notes:
    - sources_ok counts sources processed without raising an exception
      (even if 0 jobs were returned).
    - max_fetch_per_run is a global cap across ALL sources.
    """
    now = utcnow_naive()

    sources_ok = 0
    created_jobs = 0
    processed = 0

    stmt = select(Source).where(Source.is_active == 1).order_by(Source.id.asc())
    sources = session.execute(stmt).scalars().all()

    for src in sources:
        if processed >= max_fetch_per_run:
            break

        try:
            ats_type = (getattr(src, "ats_type", None) or "").strip()

            if ats_type == "lever":
                if not can_consume_call(session, "lever", max_per_day=max_calls_per_day):
                    src.last_error = "daily_api_cap_reached"
                    session.commit()
                    continue

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)

                if per_source_limit is not None:
                    postings = postings[: max(per_source_limit, 0)]

                for p in postings:
                    if processed >= max_fetch_per_run:
                        break

                    # propagate cap into upsert (only used for NEW jobs)
                    p = dict(p)
                    p["max_new_jobs_per_day"] = max_new_jobs_per_day

                    if upsert_job(session, source=src, posting=p, now=now):
                        created_jobs += 1
                    processed += 1

                src.last_ok_at = now
                src.last_error = None
                session.commit()
                sources_ok += 1
                continue

            if ats_type == "greenhouse":
                # Greenhouse uses paginated calls; count them against provider budget.
                all_postings: list[dict[str, Any]] = []

                for page in range(1, greenhouse_max_pages + 1):
                    if processed >= max_fetch_per_run:
                        break

                    if not can_consume_call(session, "greenhouse", max_per_day=max_calls_per_day):
                        src.last_error = "daily_api_cap_reached"
                        session.commit()
                        break

                    page_items = fetch_greenhouse_jobs_page(
                        src.api_base,
                        page=page,
                        timeout_s=request_timeout_s,
                        per_page=greenhouse_per_page,
                    )
                    if not page_items:
                        break
                    all_postings.extend(page_items)

                    if per_source_limit is not None and len(all_postings) >= per_source_limit:
                        all_postings = all_postings[:per_source_limit]
                        break

                for p in all_postings:
                    if processed >= max_fetch_per_run:
                        break

                    p = dict(p)
                    p["max_new_jobs_per_day"] = max_new_jobs_per_day

                    if upsert_job(session, source=src, posting=p, now=now):
                        created_jobs += 1
                    processed += 1

                src.last_ok_at = now
                if src.last_error == "daily_api_cap_reached":
                    # keep the cap error for observability
                    session.commit()
                else:
                    src.last_error = None
                    session.commit()
                    sources_ok += 1
                continue

            # Unknown provider
            src.last_error = f"unsupported_ats_type:{ats_type or 'missing'}"
            session.commit()

        except Exception as exc:  # noqa: BLE001 (operational pipeline: persist error & continue)
            src.last_error = f"ingest_failed:{type(exc).__name__}:{exc}"
            session.commit()

    return sources_ok, created_jobs
