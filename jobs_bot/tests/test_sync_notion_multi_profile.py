from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select

from jobs_bot.models import Job, JobProfile, Profile, Source
from jobs_bot.sync_notion import sync_pending_jobs


class _FakeNotion:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []
        self.queries: list[dict[str, Any]] = []

    def query_page_id(self, *, job_uid: str, profile_id: str | None = None) -> str | None:
        self.queries.append({"job_uid": job_uid, "profile_id": profile_id})
        return None

    def create_page(self, *, properties: dict[str, Any], children=None) -> str:
        self.created.append({"properties": properties})
        return "new-page-id"

    def update_page(self, *, page_id: str, properties: dict[str, Any]) -> None:
        self.updated.append({"page_id": page_id, "properties": properties})


def test_sync_pending_jobs_multi_profile_creates_pages(sqlite_session):
    # Arrange DB
    src = Source(
        ats_type="lever",
        company_slug="acme",
        company_name="ACME",
        api_base="https://api.lever.co/v0/postings/acme",
        is_active=1,
        discovered_via="manual",
    )
    sqlite_session.add(src)
    sqlite_session.flush()

    job = Job(
        job_uid="a" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="A",
        company="ACME",
        url="https://example.com",
        first_seen=dt.datetime(2026, 1, 1),
        last_seen=dt.datetime(2026, 1, 1),
        last_checked=dt.datetime(2026, 1, 2),
        raw_json={},
        fit_score=80,
        fit_class="Good",
    )
    sqlite_session.add(job)

    # Profile row is required because job_profile has FK to profiles in MySQL.
    prof = Profile(profile_id="p1", cv_path="/tmp/cv.docx", cv_sha256="0" * 64)
    sqlite_session.add(prof)

    sqlite_session.commit()

    notion = _FakeNotion()

    # Act
    n = sync_pending_jobs(sqlite_session, notion=notion, limit=10, fit_min=60, profile_id="p1")

    # Assert
    assert n == 1
    assert notion.created
    props = notion.created[0]["properties"]
    assert "Profile" in props
    assert props["Profile"]["rich_text"][0]["text"]["content"] == "p1"

    jp = sqlite_session.execute(
        select(JobProfile).where(JobProfile.job_uid == job.job_uid, JobProfile.profile_id == "p1")
    ).scalar_one()
    assert jp.notion_page_id == "new-page-id"
    assert jp.notion_last_sync is not None
