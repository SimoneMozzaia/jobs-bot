from __future__ import annotations

import datetime as dt
import json
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from .models import Job, JobEnrichment, JobProfile, Profile, Source
from .notion_client import NotionClient, NotionError


def _as_date(value: dt.datetime | None) -> str:
    if value is None:
        return dt.date.today().isoformat()
    return value.date().isoformat()


def _rt(value: str | None) -> dict[str, Any]:
    txt = (value or "").strip()
    if not txt:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": txt}}]}


def _title(value: str | None) -> dict[str, Any]:
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
    *,
    job: Job,
    job_profile: JobProfile,
    enrich: JobEnrichment | None,
    profile_id: str,
    src: Source | None = None,
) -> dict[str, Any]:
    source = getattr(job, "source", None) or src
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    score = int(job_profile.fit_score or 0)
    props: dict[str, Any] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
        "Profile": _rt(profile_id),
        "Company": _rt(job.company),
        "Job URL": {"url": job.url},
        "Status": {"status": {"name": _status_for_new_page(score)}},
        "Fit score": {"number": score},
        "Fit class": {"select": {"name": _fit_class_from_score(score)}},
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

    if job_profile.penalty_flags:
        props["Penalty flags"] = _rt(
            json.dumps(job_profile.penalty_flags, ensure_ascii=False, sort_keys=True, indent=2)
        )

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
                props["Skills required"] = {
                    "multi_select": [{"name": str(s)[:100]} for s in skills if s]
                }

    props.setdefault("Summary", {"rich_text": []})
    props.setdefault("Pros", {"rich_text": []})
    props.setdefault("Cons", {"rich_text": []})
    props.setdefault("Best outreach target", {"rich_text": []})
    props.setdefault("Contact", {"rich_text": []})

    return props


def build_properties_for_update(
    *,
    job: Job,
    job_profile: JobProfile,
    enrich: JobEnrichment | None,
    profile_id: str,
    src: Source | None = None,
) -> dict[str, Any]:
    source = getattr(job, "source", None) or src
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    score = int(job_profile.fit_score or 0)
    props: dict[str, Any] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
        "Profile": _rt(profile_id),
        "Company": _rt(job.company),
        "Fit score": {"number": score},
        "Fit class": {"select": {"name": _fit_class_from_score(score)}},
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

    if job_profile.penalty_flags is not None:
        props["Penalty flags"] = _rt(
            json.dumps(job_profile.penalty_flags, ensure_ascii=False, sort_keys=True, indent=2)
        )

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
                props["Skills required"] = {
                    "multi_select": [{"name": str(s)[:100]} for s in skills if s]
                }

    return props


def upsert_job_profile_to_notion(
    session: Session,
    notion: NotionClient,
    *,
    job: Job,
    job_profile: JobProfile,
    profile_id: str,
    now: dt.datetime,
) -> None:
    enrich = job.enrichment

    try:
        if job_profile.notion_page_id:
            props = build_properties_for_update(
                job=job,
                job_profile=job_profile,
                enrich=enrich,
                profile_id=profile_id,
            )
            notion.update_page(page_id=job_profile.notion_page_id, properties=props)
            job_profile.notion_last_error = None
            job_profile.notion_last_sync = now
            return

        existing_page_id = notion.query_page_id(job_uid=job.job_uid, profile_id=profile_id)
        if existing_page_id:
            job_profile.notion_page_id = existing_page_id
            props = build_properties_for_update(
                job=job,
                job_profile=job_profile,
                enrich=enrich,
                profile_id=profile_id,
            )
            notion.update_page(page_id=existing_page_id, properties=props)
            job_profile.notion_last_error = None
            job_profile.notion_last_sync = now
            return

        props = build_properties_for_create(
            job=job,
            job_profile=job_profile,
            enrich=enrich,
            profile_id=profile_id,
        )
        page_id = notion.create_page(properties=props)
        job_profile.notion_page_id = page_id
        job_profile.notion_last_error = None
        job_profile.notion_last_sync = now

    except NotionError as exc:
        job_profile.notion_last_error = str(exc)


def sync_pending_jobs(
    session: Session,
    *,
    notion: NotionClient,
    limit: int,
    fit_min: int,
    profile_id: str,
) -> int:
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)

    profile = session.get(Profile, profile_id)
    if profile is None:
        raise RuntimeError(f"Profile not found: {profile_id}")

    cv_sha256 = profile.cv_sha256 or ""

    stmt = (
        select(JobProfile, Job)
        .join(Job, Job.job_uid == JobProfile.job_uid)
        .options(selectinload(Job.source), selectinload(Job.enrichment))
        .where(
            JobProfile.profile_id == profile_id,
            JobProfile.fit_score >= fit_min,
            # Sync only if fit is computed for the current job/profile state
            JobProfile.fit_profile_cv_sha256 == cv_sha256,
            JobProfile.fit_job_last_checked == Job.last_checked,
            or_(
                JobProfile.notion_last_sync.is_(None),
                Job.last_checked > JobProfile.notion_last_sync,
                JobProfile.fit_computed_at > JobProfile.notion_last_sync,
            ),
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    rows = session.execute(stmt).all()
    for jp, job in rows:
        upsert_job_profile_to_notion(
            session,
            notion,
            job=job,
            job_profile=jp,
            profile_id=profile_id,
            now=now,
        )

    session.commit()
    return len(rows)
