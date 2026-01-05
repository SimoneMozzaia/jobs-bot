import datetime as dt

from jobs_bot.models import Job, JobEnrichment, JobProfile, Profile, Source
from jobs_bot.sync_notion import sync_pending_jobs


def test_sync_pending_jobs_uses_job_uid_and_profile_as_key(sqlite_session, fake_notion):
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

    profile1 = Profile(
        profile_id="p1",
        cv_path="/tmp/cv1.docx",
        cv_sha256="a" * 64,
        profile_json=None,
        profile_text="Python",
        analyzed_at=None,
        last_error=None,
    )
    profile2 = Profile(
        profile_id="p2",
        cv_path="/tmp/cv2.docx",
        cv_sha256="b" * 64,
        profile_json=None,
        profile_text="Python",
        analyzed_at=None,
        last_error=None,
    )
    sqlite_session.add(profile1)
    sqlite_session.add(profile2)

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
        raw_text="Python required",
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

    jp2 = JobProfile(
        job_uid=job.job_uid,
        profile_id="p2",
        fit_score=90,
        fit_class="Good",
        penalty_flags=None,
        fit_job_last_checked=now,
        fit_profile_cv_sha256=profile2.cv_sha256,
        fit_computed_at=now,
        notion_page_id=None,
        notion_last_sync=None,
        notion_last_error=None,
    )
    sqlite_session.add(jp2)
    sqlite_session.commit()

    created_p2 = sync_pending_jobs(
        sqlite_session,
        notion=fake_notion,
        limit=10,
        fit_min=60,
        profile_id="p2",
    )
    assert created_p2 == 1
    page_for_p2 = fake_notion.query_page_id(job_uid=job.job_uid, profile_id="p2")
    assert page_for_p2 is not None
    assert fake_notion.query_page_id(job_uid=job.job_uid, profile_id="p1") is None

    jp1 = JobProfile(
        job_uid=job.job_uid,
        profile_id="p1",
        fit_score=85,
        fit_class="Good",
        penalty_flags={"missing_languages": ["italian"]},
        fit_job_last_checked=now,
        fit_profile_cv_sha256=profile1.cv_sha256,
        fit_computed_at=now,
        notion_page_id=None,
        notion_last_sync=None,
        notion_last_error=None,
    )
    sqlite_session.add(jp1)
    sqlite_session.commit()

    created_p1 = sync_pending_jobs(
        sqlite_session,
        notion=fake_notion,
        limit=10,
        fit_min=60,
        profile_id="p1",
    )
    assert created_p1 == 1

    page_for_p1 = fake_notion.query_page_id(job_uid=job.job_uid, profile_id="p1")
    assert page_for_p1 is not None
    assert page_for_p1 != page_for_p2

    payload = fake_notion.pages[page_for_p1]
    assert "Profile" in payload
