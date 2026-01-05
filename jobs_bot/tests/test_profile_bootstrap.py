from __future__ import annotations

import hashlib
from pathlib import Path

from docx import Document

from jobs_bot.models import Job, JobProfile, Source
from jobs_bot.profile_bootstrap import bootstrap_profile


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _write_cv_docx(path: Path, *, text: str) -> str:
    doc = Document()
    for line in text.splitlines():
        doc.add_paragraph(line)
    doc.save(str(path))
    return _sha256_file(path)


def test_profile_bootstrap_creates_profile(sqlite_session, tmp_path):
    profile_id = "p1"
    cv_path = tmp_path / "cv.docx"
    sha = _write_cv_docx(cv_path, text="CV v1\nPython\nSQL")

    profile, changed = bootstrap_profile(sqlite_session, profile_id=profile_id, cv_path=str(cv_path))

    assert changed is True
    assert profile.profile_id == profile_id
    assert profile.cv_sha256 == sha
    assert profile.profile_text and "python" in profile.profile_text.lower()


def test_profile_bootstrap_invalidates_job_profiles_on_cv_change(sqlite_session, tmp_path):
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

    job = Job(
        job_uid="a" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="Backend Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=sqlite_session.bind.dialect.default_schema_name,  # not used in this test
        last_seen=sqlite_session.bind.dialect.default_schema_name,   # not used
        last_checked=sqlite_session.bind.dialect.default_schema_name,  # not used
        raw_json={},
    )
    # fix required datetime fields
    import datetime as dt
    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job.first_seen = now
    job.last_seen = now
    job.last_checked = now

    sqlite_session.add(job)
    sqlite_session.commit()

    profile_id = "p1"
    cv_path = tmp_path / "cv.docx"
    _write_cv_docx(cv_path, text="CV v1\nPython\nSQL")

    profile, _ = bootstrap_profile(sqlite_session, profile_id=profile_id, cv_path=str(cv_path))
    jp = JobProfile(
        job_uid=job.job_uid,
        profile_id=profile.profile_id,
        fit_score=80,
        fit_class="Good",
        penalty_flags={"x": 1},
        notion_page_id="n",
        notion_last_sync=now,
        notion_last_error=None,
    )
    sqlite_session.add(jp)
    sqlite_session.commit()

    sha_v2 = _write_cv_docx(cv_path, text="CV v2\nPython\nKubernetes")
    profile, changed = bootstrap_profile(sqlite_session, profile_id=profile_id, cv_path=str(cv_path))

    assert changed is True
    assert profile.cv_sha256 == sha_v2

    refreshed = sqlite_session.get(JobProfile, (job.job_uid, profile_id))
    assert refreshed is not None
    assert refreshed.fit_score == 0
    assert refreshed.fit_class == "No"
    assert refreshed.penalty_flags is None
    assert refreshed.notion_page_id == "n"
    assert refreshed.notion_last_sync is None
    assert refreshed.fit_job_last_checked is None
    assert refreshed.fit_profile_cv_sha256 is None
    assert refreshed.fit_computed_at is None
