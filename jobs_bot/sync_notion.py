from __future__ import annotations

import datetime as dt
from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from .models import Job, JobEnrichment
from .notion_client import NotionClient, NotionError

def rt(value: str | None) -> dict:
    """Notion rich_text helper. Never returns invalid rich_text items."""
    txt = (value or "").strip()
    if not txt:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": txt}}]}


def _fit_class_from_score(score: int | None) -> str:
    if score is None:
        return "No"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Maybe"
    return "No"


def _status_for_new_page(score: int | None) -> str:
    # Create-time default only; we never overwrite Status later.
    if score is not None and score >= 75:
        return "Shortlist"
    return "New"


def build_properties_for_create(job: Job, enrich: JobEnrichment | None) -> dict:
    today = dt.date.today().isoformat()

    props = {
        "Job Title": {"title": [{"text": {"content": job.title}}]},
        "Job UID": {"rich_text": [{"tjjjjjjjjjjjjjjjjjjjjjjjhymmmmmmmmmmmmmmmmmmmmmmmmmmmjext": {"content": job.job_uid}}]},
        "Job URL": {"url": job.url},

        "Status": {"status": {"name": _status_for_new_page(job.fit_score)}},

        "Fit score": {"number": job.fit_score if job.fit_score is not None else 0},
        "Fit class": {"select": {"name": _fit_class_from_score(job.fit_score)}},

        "First seen": {"date": {"start": job.first_seen.date().isoformat() if job.first_seen else today}},
        "Last checked": {"date": {"start": job.last_checked.date().isoformat() if job.last_checked else today}},
   "Company": {"rich_text": [{"text": {"content": job.company}}]},
 }

    # Optional selects/rich text
    if job.location_raw:
        props["Location"] = {"rich_text": [{"text": {"content": job.location_raw}}]}
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    # Source is select (Greenhouse/Lever/Other) — we store ats_type in Source by convention
    props["Source"] = {"select": {"name": "Other"}}  # default; ingestion will set correctly

    if job.salary_text:
        props["Salary"] = {"rich_text": [{"text": {"content": job.salary_text}}]}

    if enrich:
        if enrich.summary:
            props["Summary"] = {"rich_text": [{"text": {"content": enrich.summary}}]}
        if enrich.pros:
            props["Pros"] = {"rich_text": [{"text": {"content": enrich.pros}}]}
        if enrich.cons:
            props["Cons"] = {"rich_text": [{"text": {"content": enrich.cons}}]}
        if enrich.outreach_target:
            props["Best outreach target"] = {"rich_text": [{"text": {"content": enrich.outreach_target}}]}
        if enrich.skills_json and isinstance(enrich.skills_json, dict):
            skills = enrich.skills_json.get("skills") or []
            if isinstance(skills, list) and skills:
                props["Skills required"] = {"multi_select": [{"name": s[:100]} for s in skills if isinstance(s, str)]}

    # Ensure text fields exist to avoid “missing property type” surprises
    props.setdefault("Summary", {"rich_text": [{"text": {"content": ""}}]})
    props.setdefault("Pros", {"rich_text": [{"text": {"content": ""}}]})
    props.setdefault("Cons", {"rich_text": [{"text": {"content": ""}}]})
    props.setdefault("Best outreach target", {"rich_text": [{"text": {"content": ""}}]})
    props.setdefault("Contact", {"rich_text": [{"text": {"content": ""}}]})

    return props


def build_properties_for_update(job: Job, enrich: JobEnrichment | None) -> dict:
    # DO NOT overwrite Status.
    today = dt.date.today().isoformat()

    props = {
        "Fit score": {"number": job.fit_score if job.fit_score is not None else 0},
        "Fit class": {"select": {"name": _fit_class_from_score(job.fit_score)}},
        "Last checked": {"date": {"start": job.last_checked.date().isoformat() if job.last_checked else today}},
    "Company": {"rich_text": [{"text": {"content": job.company}}]},
}

    if job.location_raw:
        props["Location"] = {"rich_text": [{"text": {"content": job.location_raw}}]}
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    if job.salary_text:
        props["Salary"] = {"rich_text": [{"text": {"content": job.salary_text}}]}

    if enrich:
        if enrich.summary is not None:
            props["Summary"] = {"rich_text": [{"text": {"content": enrich.summary}}]}
        if enrich.pros is not None:
            props["Pros"] = {"rich_text": [{"text": {"content": enrich.pros}}]}
        if enrich.cons is not None:
            props["Cons"] = {"rich_text": [{"text": {"content": enrich.cons}}]}
        if enrich.outreach_target is not None:
            props["Best outreach target"] = {"rich_text": [{"text": {"content": enrich.outreach_target}}]}
        if enrich.skills_json and isinstance(enrich.skills_json, dict):
            skills = enrich.skills_json.get("skills") or []
            if isinstance(skills, list):
                props["Skills required"] = {"multi_select": [{"name": s[:100]} for s in skills if isinstance(s, str)]}

    return props


def upsert_job_to_notion(
    session: Session,
    notion: NotionClient,
    job: Job,
    now: dt.datetime,
) -> None:
    enrich = session.get(JobEnrichment, job.job_uid)

    try:
        if job.notion_page_id:
            props = build_properties_for_update(job, enrich)
            notion.update_page(job.notion_page_id, props)
            job.notion_last_error = None
            job.notion_last_sync = now
            return

        # no page_id → dedupe by Job UID in Notion
        existing_page_id = notion.query_by_job_uid(job.job_uid)
        if existing_page_id:
            job.notion_page_id = existing_page_id
            props = build_properties_for_update(job, enrich)
            notion.update_page(existing_page_id, props)
            job.notion_last_error = None
            job.notion_last_sync = now
            return

        # create new
        props = build_properties_for_create(job, enrich)
        page_id = notion.create_page(props)
        job.notion_page_id = page_id
        job.notion_last_error = None
        job.notion_last_sync = now

    except NotionError as e:
        job.notion_last_error = str(e)
        # do not raise: keep the batch running


def sync_pending_jobs(session: Session, notion: NotionClient, limit: int, fit_min: int) -> int:
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)

    stmt = (
        select(Job)
        .where(
            Job.fit_score.isnot(None),
            Job.fit_score >= fit_min,
            or_(Job.notion_last_sync.is_(None), Job.last_checked > Job.notion_last_sync),
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    jobs = session.execute(stmt).scalars().all()
    for j in jobs:
        upsert_job_to_notion(session, notion, j, now)

    session.commit()
    return len(jobs)
