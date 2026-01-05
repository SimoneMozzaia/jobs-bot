from __future__ import annotations

import datetime as dt

from jobs_bot.fit_scoring import compute_fit_scores_for_profile
from jobs_bot.models import Job, JobEnrichment, Profile, Source


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
        profile_id="p1",
        cv_path="/tmp/cv.docx",
        cv_sha256="a" * 64,
        profile_text="Experienced Python developer. SQL, AWS, Docker.",
        profile_json=None,
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile)
    sqlite_session.commit()

    now = dt.datetime(2026, 1, 3, 0, 0, 0)
    job = Job(
        job_uid="b" * 40,
        source_id=src.id,
        ats_job_id="1",
        title="Backend Python Engineer",
        company="ACME",
        url="https://example.com",
        first_seen=now,
        last_seen=now,
        last_checked=now,
        raw_json={},
        fit_score=0,
        fit_class="No",
    )
    sqlite_session.add(job)
    sqlite_session.commit()

    enrich = JobEnrichment(
        job_uid=job.job_uid,
        skills_json={"skills": ["Python", "SQL"]},
        summary="",
        pros="",
        cons="",
        outreach_target="",
        salary=None,
        llm_model=None,
        llm_tokens=None,
        enriched_at=now,
    )
    sqlite_session.add(enrich)
    sqlite_session.commit()

    stats = compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)

    assert stats.attempted == 1
    assert stats.inserted_jobs == 1

    from jobs_bot.models import JobProfile

    jp = sqlite_session.get(JobProfile, (job.job_uid, profile.profile_id))
    assert jp is not None
    assert jp.fit_score >= 80
    assert jp.fit_class == "Good"
    assert jp.fit_profile_cv_sha256 == profile.cv_sha256
    assert jp.fit_job_last_checked == job.last_checked

    stats2 = compute_fit_scores_for_profile(sqlite_session, profile=profile, limit=10)
    assert stats2.attempted == 0
