from __future__ import annotations

import datetime as dt

from jobs_bot.models import Job, JobEnrichment, Source
from jobs_bot.sync_notion import build_properties_for_create, build_properties_for_update, sync_pending_jobs


def test_build_properties_are_valid_for_empty_rich_text(sqlite_session):
    src = Source(
        ats_type="lever",
        company_slug="acme",
        company_name="ACME",
        api_base="https://api.lever.co/v0/postings/acme",
        is_active=1,
        discovered_via="manual",
    )
    sqlite_session.add(src)
    sqlite_session.commit()

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="a" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        fit_score=80,
        fit_class="Good",
    )
    sqlite_session.add(job)
    sqlite_session.commit()

    enr = JobEnrichment(job_uid=job.job_uid, salary="€ 100,000")
    sqlite_session.add(enr)
    sqlite_session.commit()

    props = build_properties_for_create(job, enrich=enr, src=src)
    assert props["Job UID"]["rich_text"][0]["text"]["content"] == job.job_uid
    assert props["Salary"]["rich_text"][0]["text"]["content"] == "€ 100,000"

    props_u = build_properties_for_update(job, enrich=enr, src=src)
    assert "Status" not in props_u


def test_sync_pending_jobs_creates_and_updates(sqlite_session, fake_notion):
    src = Source(
        ats_type="lever",
        company_slug="acme",
        company_name="ACME",
        api_base="https://api.lever.co/v0/postings/acme",
        is_active=1,
        discovered_via="manual",
    )
    sqlite_session.add(src)
    sqlite_session.commit()

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="b" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="Backend Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        fit_score=70,
        fit_class="Maybe",
    )
    sqlite_session.add(job)
    sqlite_session.commit()

    n = sync_pending_jobs(sqlite_session, notion=fake_notion, limit=10, fit_min=60)
    assert n == 1
    sqlite_session.refresh(job)
    assert job.notion_page_id is not None
    assert job.notion_last_sync is not None
    assert job.notion_last_error is None

    # Trigger an update by advancing last_checked beyond the last sync timestamp
    sqlite_session.refresh(job)
    assert job.notion_last_sync is not None
    job.title = "Backend Engineer II"
    job.last_checked = job.notion_last_sync + dt.timedelta(seconds=1)
    sqlite_session.commit()

    n2 = sync_pending_jobs(sqlite_session, notion=fake_notion, limit=10, fit_min=60)
    assert n2 == 1
    assert fake_notion.updated_payloads
    last_props = fake_notion.updated_payloads[-1]
    assert "Status" not in last_props
