from __future__ import annotations

import hashlib
from pathlib import Path

import datetime as dt

from jobs_bot.models import Job, JobProfile, Profile, Source
from jobs_bot.profile_bootstrap import bootstrap_profile


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def test_bootstrap_profile_creates_profile(sqlite_session, tmp_path):
    cv_bytes = b"cv-v1"
    profile_dir = tmp_path / "profiles" / "default"
    profile_dir.mkdir(parents=True)
    cv_path = profile_dir / "cv.docx"
    cv_path.write_bytes(cv_bytes)

    profile, changed = bootstrap_profile(
        sqlite_session,
        profile_id="default",
        cv_path=str(cv_path),
    )

    assert changed is True
    assert profile.profile_id == "default"
    assert profile.cv_path == str(cv_path)
    assert profile.cv_sha256 == _sha256_bytes(cv_bytes)

    db_profile = sqlite_session.get(Profile, "default")
    assert db_profile is not None
    assert db_profile.cv_sha256 == _sha256_bytes(cv_bytes)


def test_bootstrap_profile_invalidates_job_profile_on_cv_change(sqlite_session, tmp_path):
    profile_id = "default"

    cv_v1 = b"cv-v1"
    cv_v2 = b"cv-v2"

    profile_dir = tmp_path / "profiles" / profile_id
    profile_dir.mkdir(parents=True)
    cv_path = profile_dir / "cv.docx"
    cv_path.write_bytes(cv_v1)

    bootstrap_profile(sqlite_session, profile_id=profile_id, cv_path=str(cv_path))

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

    jp = JobProfile(
        job_uid=job.job_uid,
        profile_id=profile_id,
        fit_score=88,
        fit_class="Good",
        penalty_flags={"x": True},
        notion_page_id="page-123",
        notion_last_sync=now,
        notion_last_error="err",
    )
    sqlite_session.add(jp)
    sqlite_session.commit()

    cv_path.write_bytes(cv_v2)
    profile, changed = bootstrap_profile(sqlite_session, profile_id=profile_id, cv_path=str(cv_path))

    assert changed is True
    assert profile.cv_sha256 == _sha256_bytes(cv_v2)

    jp2 = sqlite_session.get(JobProfile, {"job_uid": job.job_uid, "profile_id": profile_id})
    assert jp2 is not None
    assert jp2.notion_page_id == "page-123"
    assert jp2.fit_score == 0
    assert jp2.fit_class == "No"
    assert jp2.penalty_flags is None
    assert jp2.notion_last_sync is None
    assert jp2.notion_last_error is None
