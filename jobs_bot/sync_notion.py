from __future__ import annotations

import datetime as dt
import json

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from .models import Job, JobEnrichment, Source
from .notion_client import NotionClient, NotionError


def _as_date(value: dt.datetime | None) -> str:
    if value is None:
        return dt.date.today().isoformat()
    return value.date().isoformat()


def _rt(value: str | None) -> dict:
    txt = (value or "").strip()
    if not txt:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": txt}}]}


def _title(value: str | None) -> dict:
    txt = (value or "").strip() or "Untitled"
    return {"title": [{"text": {"content": txt}}]}


def _fit_class_from_score(score: int | None) -> str:
    if score is None:
        return "No"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Maybe"
    return "No"


def _status_for_new_page(score: int | None) -> str:
    if score is None:
        return "New"
    if score >= 75:
        return "Shortlist"
    if score >= 60:
        return "New"
    return "Rejected"


def _source_label(source: Source | None) -> str:
    ats_type = getattr(source, "ats_type", None)
    if ats_type == "greenhouse":
        return "Greenhouse"
    if ats_type == "lever":
        return "Lever"
    return "Other"


def _region_multi_select(source: Source | None) -> list[dict[str, str]]:
    hint = getattr(source, "region_hint", None)
    if not hint:
        return []
    return [{"name": str(hint)[:100]}]


def build_properties_for_create(
    job: Job,
    enrich: JobEnrichment | None = None,
    *,
    src: Source | None = None,
) -> dict:
    source = getattr(job, "source", None) or src
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    props: dict[str, dict] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
        "Company": _rt(job.company),
        "Job URL": {"url": job.url},
        "Status": {"status": {"name": _status_for_new_page(job.fit_score)}},
        "Fit score": {"number": int(job.fit_score or 0)},
        "Fit class": {"select": {"name": _fit_class_from_score(job.fit_score)}},
        "First seen": {"date": {"start": _as_date(job.first_seen)}},
        "Last checked": {"date": {"start": _as_date(job.last_checked)}},
        "Source": {"select": {"name": _source_label(source)}},
        "Region": {"multi_select": _region_multi_select(source)},
    }

    if job.location_raw:
        props["Location"] = _rt(job.location_raw)
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    if salary:
        props["Salary"] = _rt(salary)

    if job.penalty_flags:
        props["Penalty flags"] = _rt(json.dumps(job.penalty_flags, ensure_ascii=False, sort_keys=True, indent=2))

    if enrich:
        if enrich.summary:
            props["Summary"] = _rt(enrich.summary)
        if enrich.pros:
            props["Pros"] = _rt(enrich.pros)
        if enrich.cons:
            props["Cons"] = _rt(enrich.cons)
        if enrich.outreach_target:
            props["Best outreach target"] = _rt(enrich.outreach_target)
        if enrich.skills_json and isinstance(enrich.skills_json, dict):
            skills = enrich.skills_json.get("skills") or []
            if isinstance(skills, list) and skills:
                props["Skills required"] = {"multi_select": [{"name": str(s)[:100]} for s in skills if s]}

    props.setdefault("Summary", {"rich_text": []})
    props.setdefault("Pros", {"rich_text": []})
    props.setdefault("Cons", {"rich_text": []})
    props.setdefault("Best outreach target", {"rich_text": []})
    props.setdefault("Contact", {"rich_text": []})

    return props


def build_properties_for_update(
    job: Job,
    enrich: JobEnrichment | None = None,
    *,
    src: Source | None = None,
) -> dict:
    source = getattr(job, "source", None) or src
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    props: dict[str, dict] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
        "Company": _rt(job.company),
        "Fit score": {"number": int(job.fit_score or 0)},
        "Fit class": {"select": {"name": _fit_class_from_score(job.fit_score)}},
        "Last checked": {"date": {"start": _as_date(job.last_checked)}},
        "Source": {"select": {"name": _source_label(source)}},
        "Region": {"multi_select": _region_multi_select(source)},
    }

    if job.location_raw:
        props["Location"] = _rt(job.location_raw)
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    if salary:
        props["Salary"] = _rt(salary)

    if job.penalty_flags:
        props["Penalty flags"] = _rt(json.dumps(job.penalty_flags, ensure_ascii=False, sort_keys=True, indent=2))

    if enrich:
        if enrich.summary is not None:
            props["Summary"] = _rt(enrich.summary)
        if enrich.pros is not None:
            props["Pros"] = _rt(enrich.pros)
        if enrich.cons is not None:
            props["Cons"] = _rt(enrich.cons)
        if enrich.outreach_target is not None:
            props["Best outreach target"] = _rt(enrich.outreach_target)
        if enrich.skills_json and isinstance(enrich.skills_json, dict):
            skills = enrich.skills_json.get("skills") or []
            if isinstance(skills, list):
                props["Skills required"] = {"multi_select": [{"name": str(s)[:100]} for s in skills if s]}

    return props


def upsert_job_to_notion(session: Session, notion: NotionClient, job: Job, now: dt.datetime) -> None:
    enrich = job.enrichment

    try:
        if job.notion_page_id:
            props = build_properties_for_update(job, enrich)
            notion.update_page(job.notion_page_id, props)
            job.notion_last_error = None
            job.notion_last_sync = now
            return

        existing_page_id = notion.query_by_job_uid(job.job_uid)
        if existing_page_id:
            job.notion_page_id = existing_page_id
            props = build_properties_for_update(job, enrich)
            notion.update_page(existing_page_id, props)
            job.notion_last_error = None
            job.notion_last_sync = now
            return

        props = build_properties_for_create(job, enrich)
        page_id = notion.create_page(props)
        job.notion_page_id = page_id
        job.notion_last_error = None
        job.notion_last_sync = now

    except NotionError as e:
        job.notion_last_error = str(e)


def sync_pending_jobs(session: Session, *, notion: NotionClient, limit: int, fit_min: int) -> int:
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)

    stmt = (
        select(Job)
        .options(selectinload(Job.source), selectinload(Job.enrichment))
        .where(
            Job.fit_score >= fit_min,
            or_(Job.notion_last_sync.is_(None), Job.last_checked > Job.notion_last_sync),
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    jobs = session.execute(stmt).scalars().all()
    for job in jobs:
        upsert_job_to_notion(session, notion, job, now)

    session.commit()
    return len(jobs)
