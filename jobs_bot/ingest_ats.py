from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .api_usage import can_consume_call, can_create_new_job
from .ats_clients import (
    fetch_greenhouse_jobs_page,
    fetch_lever_postings,
    fetch_successfactors_postings,
    fetch_workable_jobs,
    fetch_workday_jobs_page,
)
from .models import Job, Source


def _job_uid(*, ats_type: str, company_slug: str, ats_job_id: str) -> str:
    key = f"{ats_type}:{company_slug}:{ats_job_id}".encode("utf-8")
    return hashlib.sha1(key).hexdigest()


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
    Fetch jobs from all active sources and upsert into jobs.

    Supported source.ats_type:
      - lever
      - greenhouse
      - workday (public career-site feed via wday/cxs)
      - successfactors (public XML job feed)
      - workable (public company feed)

    Returns: (sources_ok_count, new_jobs_created_count)
    """
    sources_ok = 0
    created = 0
    items_processed = 0

    sources = session.execute(select(Source).where(Source.is_active == 1)).scalars().all()

    for src in sources:
        if items_processed >= max_fetch_per_run:
            break

        ats_type = (src.ats_type or "").strip().lower()
        src.last_error = None
        session.flush()

        # How many more items we can accept this run (global cap)
        remaining_capacity = max_fetch_per_run - items_processed
        if remaining_capacity <= 0:
            break

        try:
            postings: list[dict[str, Any]] = []

            if ats_type == "lever":
                if not can_consume_call(session, ats_type, max_per_day=max_calls_per_day):
                    src.last_error = "daily_api_cap_reached"
                    session.commit()
                    continue

                postings = fetch_lever_postings(src.api_base, timeout_s=request_timeout_s)

            elif ats_type == "greenhouse":
                # Greenhouse uses paginated fetch; we count each page as an API call.
                for page in range(1, greenhouse_max_pages + 1):
                    if not can_consume_call(session, ats_type, max_per_day=max_calls_per_day):
                        src.last_error = "daily_api_cap_reached"
                        session.commit()
                        break

                    page_jobs = fetch_greenhouse_jobs_page(
                        src.api_base,
                        page=page,
                        timeout_s=request_timeout_s,
                        per_page=greenhouse_per_page,
                    )
                    if not page_jobs:
                        break

                    postings.extend(page_jobs)

                    if per_source_limit is not None and len(postings) >= per_source_limit:
                        postings = postings[:per_source_limit]
                        break

                    if len(postings) >= remaining_capacity:
                        postings = postings[:remaining_capacity]
                        break

            elif ats_type == "workday":
                # Workday public feed typically uses limit/offset pagination.
                # We reuse greenhouse_per_page / greenhouse_max_pages as generic paging controls.
                per_page = max(1, int(greenhouse_per_page))
                for page_idx in range(greenhouse_max_pages):
                    if not can_consume_call(session, "workday", max_per_day=max_calls_per_day):
                        src.last_error = "daily_api_cap_reached"
                        session.commit()
                        break

                    offset = page_idx * per_page
                    page_jobs = fetch_workday_jobs_page(
                        src.api_base,
                        offset=offset,
                        limit=per_page,
                        timeout_s=request_timeout_s,
                    )
                    if not page_jobs:
                        break

                    postings.extend(page_jobs)

                    if per_source_limit is not None and len(postings) >= per_source_limit:
                        postings = postings[:per_source_limit]
                        break

                    if len(postings) >= remaining_capacity:
                        postings = postings[:remaining_capacity]
                        break

            elif ats_type == "successfactors":
                if not can_consume_call(session, "successfactors", max_per_day=max_calls_per_day):
                    src.last_error = "daily_api_cap_reached"
                    session.commit()
                    continue

                postings = fetch_successfactors_postings(src.api_base, timeout_s=request_timeout_s)

            elif ats_type == "workable":
                if not can_consume_call(session, "workable", max_per_day=max_calls_per_day):
                    src.last_error = "daily_api_cap_reached"
                    session.commit()
                    continue

                postings = fetch_workable_jobs(src.api_base, timeout_s=request_timeout_s)

            else:
                src.last_error = f"unsupported_ats_type:{ats_type}"
                session.commit()
                continue

            # Apply caps
            if per_source_limit is not None:
                postings = postings[:per_source_limit]
            postings = postings[:remaining_capacity]

            # Upsert jobs
            for p in postings:
                if items_processed >= max_fetch_per_run:
                    break

                if not can_create_new_job(session, max_new_per_day=max_new_jobs_per_day):
                    src.last_error = "daily_new_jobs_cap_reached"
                    session.commit()
                    break

                ats_job_id = str(p["ats_job_id"])
                job_uid = _job_uid(ats_type=ats_type, company_slug=src.company_slug, ats_job_id=ats_job_id)

                existing = session.get(Job, job_uid)
                now = dt.datetime.utcnow()

                if existing is None:
                    job = Job(
                        job_uid=job_uid,
                        source_id=src.id,
                        ats_job_id=ats_job_id,
                        title=p.get("title") or "",
                        company=src.company_name,
                        url=p.get("url") or "",
                        location_raw=p.get("location_raw"),
                        workplace_raw=p.get("workplace_raw"),
                        salary_text=p.get("salary_text"),
                        raw_json=p.get("raw_json") or {},
                        raw_text=p.get("raw_text"),
                        first_seen=now,
                        last_seen=now,
                        last_checked=now,
                        fit_score=0,
                        fit_class="No",
                    )
                    session.add(job)
                    created += 1
                else:
                    existing.title = p.get("title") or existing.title
                    existing.url = p.get("url") or existing.url
                    existing.location_raw = p.get("location_raw") or existing.location_raw
                    existing.workplace_raw = p.get("workplace_raw") or existing.workplace_raw
                    existing.salary_text = p.get("salary_text") or existing.salary_text
                    existing.raw_json = p.get("raw_json") or existing.raw_json
                    existing.raw_text = p.get("raw_text") or existing.raw_text
                    existing.last_seen = now
                    existing.last_checked = now

                items_processed += 1

            session.commit()
            sources_ok += 1

        except Exception as e:  # noqa: BLE001
            src.last_error = f"ingest_error:{type(e).__name__}"
            session.commit()

    return sources_ok, created
