from __future__ import annotations

import datetime as dt
import json
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import and_, or_, select, text
from sqlalchemy.orm import Session, selectinload

from .models import Job, JobEnrichment, JobProfile, Source
from .notion_client import NotionClient, NotionError


@runtime_checkable
class _LegacyNotionQuery(Protocol):
    def query_by_job_uid(self, job_uid: str) -> str | None: ...


@runtime_checkable
class _ProfileNotionQuery(Protocol):
    def query_page_id(self, *, job_uid: str, profile_id: str | None = None) -> str | None: ...


def _utcnow_naive() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


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


def _query_existing_page_id(
    notion: NotionClient,
    *,
    job_uid: str,
    profile_id: str | None,
) -> str | None:
    """
    Backward-compatible Notion lookup.

    - Legacy single-profile tests/fakes typically implement `query_by_job_uid(job_uid)`.
    - Multi-profile uses `query_page_id(job_uid=..., profile_id=...)`.

    This keeps existing tests working without requiring changes to their fakes.
    """
    if profile_id is None:
        if isinstance(notion, _LegacyNotionQuery):
            return notion.query_by_job_uid(job_uid)
        if isinstance(notion, _ProfileNotionQuery):
            return notion.query_page_id(job_uid=job_uid, profile_id=None)
        raise AttributeError("Notion client must implement query_by_job_uid or query_page_id")

    # Multi-profile lookup
    if isinstance(notion, _ProfileNotionQuery):
        return notion.query_page_id(job_uid=job_uid, profile_id=profile_id)

    # No safe way to filter by profile if the client is legacy-only.
    # We intentionally return None to avoid collisions.
    return None


def build_properties_for_create(
    job: Job,
    enrich: JobEnrichment | None = None,
    *,
    src: Source | None = None,
    profile_id: str | None = None,
    fit_score: int | None = None,
    penalty_flags: dict | None = None,
) -> dict[str, Any]:
    source = getattr(job, "source", None) or src
    score = int(fit_score if fit_score is not None else (job.fit_score or 0))
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    props: dict[str, Any] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
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

    if profile_id is not None:
        props["Profile"] = _rt(profile_id)

    if job.location_raw:
        props["Location"] = _rt(job.location_raw)
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    if salary:
        props["Salary"] = _rt(salary)

    flags = penalty_flags if penalty_flags is not None else job.penalty_flags
    if flags:
        props["Penalty flags"] = _rt(
            json.dumps(flags, ensure_ascii=False, sort_keys=True, indent=2)
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
    job: Job,
    enrich: JobEnrichment | None = None,
    *,
    src: Source | None = None,
    profile_id: str | None = None,
    fit_score: int | None = None,
    penalty_flags: dict | None = None,
) -> dict[str, Any]:
    source = getattr(job, "source", None) or src
    score = int(fit_score if fit_score is not None else (job.fit_score or 0))
    salary = (getattr(enrich, "salary", None) or job.salary_text or "").strip() or None

    props: dict[str, Any] = {
        "Job Title": _title(job.title),
        "Job UID": _rt(job.job_uid),
        "Company": _rt(job.company),
        "Fit score": {"number": score},
        "Fit class": {"select": {"name": _fit_class_from_score(score)}},
        "Last checked": {"date": {"start": _as_date(job.last_checked)}},
        "Source": {"select": {"name": _source_label(source)}},
        "Region": {"multi_select": _region_multi_select(source)},
    }

    if profile_id is not None:
        props["Profile"] = _rt(profile_id)

    if job.location_raw:
        props["Location"] = _rt(job.location_raw)
    if job.workplace_raw:
        props["Workplace"] = {"select": {"name": job.workplace_raw}}
    if salary:
        props["Salary"] = _rt(salary)

    flags = penalty_flags if penalty_flags is not None else job.penalty_flags
    if flags:
        props["Penalty flags"] = _rt(
            json.dumps(flags, ensure_ascii=False, sort_keys=True, indent=2)
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


def _ensure_job_profile_rows(session: Session, *, profile_id: str) -> None:
    """
    Ensure (job_uid, profile_id) rows exist for all jobs.

    Seeds per-profile values from legacy `jobs` fields to preserve backward compatibility
    until point (2) replaces scoring logic fully.
    """
    session.execute(
        text(
            """
            INSERT INTO job_profile (job_uid, profile_id, fit_score, fit_class, penalty_flags)
            SELECT j.job_uid, :profile_id, j.fit_score, j.fit_class, j.penalty_flags
            FROM jobs j
            WHERE NOT EXISTS (
                SELECT 1
                FROM job_profile jp
                WHERE jp.job_uid = j.job_uid AND jp.profile_id = :profile_id
            )
            """
        ),
        {"profile_id": profile_id},
    )


def upsert_job_to_notion(
    session: Session,
    notion: NotionClient,
    job: Job,
    now: dt.datetime,
    *,
    profile_id: str | None = None,
    job_profile: JobProfile | None = None,
) -> None:
    enrich = job.enrichment

    try:
        if profile_id is None:
            # Legacy single-profile behavior (must remain compatible with existing tests)
            if job.notion_page_id:
                props = build_properties_for_update(job, enrich)
                notion.update_page(page_id=job.notion_page_id, properties=props)
                job.notion_last_error = None
                job.notion_last_sync = now
                return

            existing = _query_existing_page_id(notion, job_uid=job.job_uid, profile_id=None)
            if existing:
                job.notion_page_id = existing
                props = build_properties_for_update(job, enrich)
                notion.update_page(page_id=existing, properties=props)
                job.notion_last_error = None
                job.notion_last_sync = now
                return

            props = build_properties_for_create(job, enrich)
            page_id = notion.create_page(properties=props)
            job.notion_page_id = page_id
            job.notion_last_error = None
            job.notion_last_sync = now
            return

        # Multi-profile behavior (store mapping per profile)
        if job_profile is None:
            raise RuntimeError("job_profile is required when profile_id is provided")

        if job_profile.notion_page_id:
            props = build_properties_for_update(
                job,
                enrich,
                profile_id=profile_id,
                fit_score=job_profile.fit_score,
                penalty_flags=job_profile.penalty_flags,
            )
            notion.update_page(page_id=job_profile.notion_page_id, properties=props)
            job_profile.notion_last_error = None
            job_profile.notion_last_sync = now
            return

        existing = _query_existing_page_id(notion, job_uid=job.job_uid, profile_id=profile_id)
        if not existing and profile_id == "default":
            # Backward-compat: attach legacy pages created before Profile existed
            existing = _query_existing_page_id(notion, job_uid=job.job_uid, profile_id=None)

        if existing:
            job_profile.notion_page_id = existing
            props = build_properties_for_update(
                job,
                enrich,
                profile_id=profile_id,
                fit_score=job_profile.fit_score,
                penalty_flags=job_profile.penalty_flags,
            )
            notion.update_page(page_id=existing, properties=props)
            job_profile.notion_last_error = None
            job_profile.notion_last_sync = now
            return

        props = build_properties_for_create(
            job,
            enrich,
            profile_id=profile_id,
            fit_score=job_profile.fit_score,
            penalty_flags=job_profile.penalty_flags,
        )
        page_id = notion.create_page(properties=props)
        job_profile.notion_page_id = page_id
        job_profile.notion_last_error = None
        job_profile.notion_last_sync = now

    except NotionError as exc:
        if profile_id is None:
            job.notion_last_error = str(exc)
        else:
            job_profile.notion_last_error = str(exc)


def sync_pending_jobs(
    session: Session,
    *,
    notion: NotionClient,
    limit: int,
    fit_min: int,
    profile_id: str | None = None,
) -> int:
    now = _utcnow_naive()

    if profile_id is None:
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
            upsert_job_to_notion(session, notion, job, now, profile_id=None)
        session.commit()
        return len(jobs)

    _ensure_job_profile_rows(session, profile_id=profile_id)
    session.flush()

    stmt = (
        select(Job, JobProfile)
        .join(
            JobProfile,
            and_(JobProfile.job_uid == Job.job_uid, JobProfile.profile_id == profile_id),
        )
        .options(selectinload(Job.source), selectinload(Job.enrichment))
        .where(
            JobProfile.fit_score >= fit_min,
            or_(JobProfile.notion_last_sync.is_(None), Job.last_checked > JobProfile.notion_last_sync),
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    rows = session.execute(stmt).all()
    for job, jp in rows:
        upsert_job_to_notion(session, notion, job, now, profile_id=profile_id, job_profile=jp)

    session.commit()
    return len(rows)
