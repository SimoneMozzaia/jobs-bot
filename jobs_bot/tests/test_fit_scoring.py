from __future__ import annotations

import datetime as dt

from jobs_bot.fit_scoring import compute_fit_scores_for_profile
from jobs_bot.models import Job, JobEnrichment, JobProfile, Profile, Source


def test_compute_fit_scores_for_profile_creates_job_profile_rows(sqlite_session):
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

    profile = Profile(
        profile_id="default",
        cv_path="/tmp/cv.docx",
        cv_sha256="a" * 64,
        profile_json=None,
        profile_text="Python SQL Docker AWS",
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile)

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="f" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="Backend Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        raw_text="We need Python and AWS skills",
        fit_score=0,
        fit_class="No",
    )
    sqlite_session.add(job)
    sqlite_session.add(
        JobEnrichment(
            job_uid=job.job_uid,
            skills_json={"skills": ["Python", "AWS", "Docker"]},
            summary="",
            pros="",
            cons="",
            outreach_target="",
        )
    )
    sqlite_session.commit()

    stats = compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)
    assert stats.attempted == 1
    assert stats.inserted_jobs == 1
    assert stats.updated_jobs == 0

    jp = sqlite_session.get(JobProfile, {"job_uid": job.job_uid, "profile_id": "default"})
    assert jp is not None
    assert jp.fit_score >= 80
    assert jp.fit_class == "Good"
    assert jp.fit_profile_cv_sha256 == profile.cv_sha256

    stats2 = compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)
    assert stats2.attempted == 0


def test_fit_scoring_penalizes_missing_required_language(sqlite_session):
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

    profile = Profile(
        profile_id="p1",
        cv_path="/tmp/cv.docx",
        cv_sha256="b" * 64,
        profile_json=None,
        profile_text="Python SQL English",
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile)

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="1" * 40,
        source_id=src.id,
        ats_job_id="10",
        title="Backend Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        raw_text="Fluent Italian required. Python developer.",
        fit_score=0,
        fit_class="No",
    )
    sqlite_session.add(job)
    sqlite_session.add(
        JobEnrichment(
            job_uid=job.job_uid,
            skills_json={"skills": ["Python"]},
            summary="",
            pros="",
            cons="",
            outreach_target="",
        )
    )
    sqlite_session.commit()

    stats = compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)
    assert stats.attempted == 1

    jp = sqlite_session.get(JobProfile, {"job_uid": job.job_uid, "profile_id": "p1"})
    assert jp is not None
    assert jp.penalty_flags is not None
    assert "missing_languages" in jp.penalty_flags
    assert "italian" in jp.penalty_flags["missing_languages"]


def test_fit_scoring_penalizes_location_mismatch_when_profile_has_location(sqlite_session):
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

    profile = Profile(
        profile_id="p2",
        cv_path="/tmp/cv.docx",
        cv_sha256="c" * 64,
        profile_json=None,
        profile_text="Milano Italy Python",
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile)

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="2" * 40,
        source_id=src.id,
        ats_job_id="20",
        title="Backend Engineer",
        company="ACME",
        url="https://example.com",
        location_raw="Paris, France",
        workplace_raw="Onsite",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        raw_text="Onsite role in Paris office.",
        fit_score=0,
        fit_class="No",
    )
    sqlite_session.add(job)
    sqlite_session.add(
        JobEnrichment(
            job_uid=job.job_uid,
            skills_json={"skills": ["Python"]},
            summary="",
            pros="",
            cons="",
            outreach_target="",
        )
    )
    sqlite_session.commit()

    compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)

    jp = sqlite_session.get(JobProfile, {"job_uid": job.job_uid, "profile_id": "p2"})
    assert jp is not None
    assert jp.penalty_flags is not None
    assert "location_mismatch" in jp.penalty_flags


def test_fit_scoring_penalizes_seniority_mismatch(sqlite_session):
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

    profile = Profile(
        profile_id="p3",
        cv_path="/tmp/cv.docx",
        cv_sha256="d" * 64,
        profile_json=None,
        profile_text="1 year of experience Python",
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile)

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="3" * 40,
        source_id=src.id,
        ats_job_id="30",
        title="Senior Backend Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        raw_text="Senior role. Python required.",
        fit_score=0,
        fit_class="No",
    )
    sqlite_session.add(job)
    sqlite_session.add(
        JobEnrichment(
            job_uid=job.job_uid,
            skills_json={"skills": ["Python"]},
            summary="",
            pros="",
            cons="",
            outreach_target="",
        )
    )
    sqlite_session.commit()

    compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)

    jp = sqlite_session.get(JobProfile, {"job_uid": job.job_uid, "profile_id": "p3"})
    assert jp is not None
    assert jp.penalty_flags is not None
    assert "seniority_mismatch" in jp.penalty_flags
