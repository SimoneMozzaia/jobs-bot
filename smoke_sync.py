from __future__ import annotations

import datetime as dt
import hashlib
import json

from sqlalchemy import select

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.models import Job, JobEnrichment, Source
from jobs_bot.notion_client import NotionClient
from jobs_bot.sync_notion import sync_pending_jobs


def sha1_uid(ats_type: str, company_slug: str, job_id: str) -> str:
    key = f"{ats_type}:{company_slug}:{job_id}".encode("utf-8")
    return hashlib.sha1(key).hexdigest()


def main() -> None:
    settings = get_settings()
    SessionLocal = make_session_factory(settings)

    notion = NotionClient(
        token=settings.notion_token,
        version=settings.notion_version,
        data_source_id=settings.notion_data_source_id,
        timeout_s=settings.request_timeout_s,
    )

    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)

    with SessionLocal() as session:
        ats_type = "lever"
        company_slug = "testco"
        api_base = "https://api.lever.co/v0/postings/testco"

        src = session.execute(
            select(Source).where(Source.ats_type == ats_type, Source.company_slug == company_slug)
        ).scalar_one_or_none()

        if not src:
            src = Source(
                ats_type=ats_type,
                company_slug=company_slug,
                company_name="TestCo",
                api_base=api_base,
                is_active=1,
                discovered_via="manual",
            )
            session.add(src)
            session.commit()

        job_id = "TEST-001"
        job_uid = sha1_uid(ats_type, company_slug, job_id)

        job = session.get(Job, job_uid)
        if not job:
            job = Job(
                job_uid=job_uid,
                source_id=src.id,
                ats_job_id=job_id,
                title="TEST — IT Service Quality Manager (MySQL→Notion)",
                company="TestCo",
                url="https://example.com/jobs/test-001",
                location_raw="Remote (EU)",
                workplace_raw="Remote",
                posted_at=None,
                first_seen=now,
                last_seen=now,
                last_checked=now,
                raw_json={"test": True, "provider": "smoke", "job_id": job_id},
                raw_text=json.dumps({"test": True}),
                fit_score=88,
                fit_class="Good",
                penalty_flags={"us_only": False, "work_auth": False},
                salary_text="Not disclosed",
                status="New",
            )
            session.add(job)
        else:
            job.last_seen = now
            job.last_checked = now
            job.fit_score = 88
            job.fit_class = "Good"

        enr = session.get(JobEnrichment, job_uid)
        if not enr:
            enr = JobEnrichment(
                job_uid=job_uid,
                summary="Smoke test: record created in MySQL and synced to Notion.",
                skills_json={"skills": ["ITSM", "Incident Management", "Reporting"]},
                pros="Validates DB + Notion upsert pipeline.",
                cons="Test entry only.",
                outreach_target="GCC Quality Manager / Head of ITSM",
                salary="Not disclosed",
                llm_model=None,
                llm_tokens=None,
                enriched_at=now,
            )
            session.add(enr)
        else:
            enr.summary = "Smoke test: updated and re-synced."
            enr.enriched_at = now

        session.commit()

        n = sync_pending_jobs(
            session,
            notion=notion,
            limit=10,
            fit_min=settings.fit_min,
        )
        print(f"Synced {n} job(s) to Notion.")

        job = session.get(Job, job_uid)
        print("Job UID:", job.job_uid)
        print("Notion page id:", job.notion_page_id)
        print("Notion last sync:", job.notion_last_sync)
        print("Notion last error:", job.notion_last_error)


if __name__ == "__main__":
    main()
