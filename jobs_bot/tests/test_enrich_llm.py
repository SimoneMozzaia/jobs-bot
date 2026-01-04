from __future__ import annotations

import datetime as dt

from jobs_bot.enrich_llm import enrich_pending_jobs
from jobs_bot.models import Job, JobEnrichment, Source


class DummyClient:
    def __init__(self) -> None:
        self.calls: int = 0

    def enrich_job(
        self,
        *,
        title: str,
        company: str,
        location_raw: str | None,
        workplace_raw: str | None,
        url: str,
        salary_text: str | None,
        raw_text: str | None,
    ):
        self.calls += 1
        return type(
            "DummyEnrichment",
            (),
            {
                "summary": "Short summary",
                "skills": ["Python", "SQLAlchemy"],
                "pros": ["Good scope"],
                "cons": ["Unclear level"],
                "outreach_target": "Hiring Manager",
                "salary": "EUR 50k-70k / year",
                "model": "dummy-model",
                "total_tokens": 123,
            },
        )()


def test_enrich_creates_row(sqlite_session):
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
        first_seen=dt.datetime(2026, 1, 1),
        last_seen=dt.datetime(2026, 1, 1),
        last_checked=dt.datetime(2026, 1, 2),
        raw_json={},
        fit_score=80,
        fit_class="Good",
        salary_text="€60k",
        raw_text="We need Python and SQLAlchemy.",
    )
    sqlite_session.add(job)
    sqlite_session.commit()

    client = DummyClient()
    stats = enrich_pending_jobs(sqlite_session, client=client, limit=10)

    assert stats.attempted == 1
    assert stats.enriched == 1
    assert stats.failed == 0
    assert client.calls == 1

    enr = sqlite_session.get(JobEnrichment, job.job_uid)
    assert enr is not None
    assert enr.summary == "Short summary"
    assert enr.skills_json == ["Python", "SQLAlchemy"]
    assert enr.pros and "Good scope" in enr.pros
    assert enr.salary == "EUR 50k-70k / year"
    assert enr.llm_model == "dummy-model"
    assert enr.llm_tokens == 123
    assert enr.enriched_at is not None


def test_enrich_skips_if_up_to_date(sqlite_session):
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

    job_uid = "b" * 40
    job = Job(
        job_uid=job_uid,
        source_id=src.id,
        ats_job_id="2",
        title="Data Engineer",
        company="ACME",
        url="https://example.com/2",
        first_seen=dt.datetime(2026, 1, 1),
        last_seen=dt.datetime(2026, 1, 1),
        last_checked=dt.datetime(2026, 1, 1),
        raw_json={},
        fit_score=80,
        fit_class="Good",
    )
    sqlite_session.add(job)

    enr = JobEnrichment(
        job_uid=job_uid,
        summary="Existing",
        skills_json=["Old"],
        enriched_at=dt.datetime(2026, 1, 2),  # newer than last_checked => up to date
        llm_model="dummy",
        llm_tokens=1,
    )
    sqlite_session.add(enr)
    sqlite_session.commit()

    client = DummyClient()
    stats = enrich_pending_jobs(sqlite_session, client=client, limit=10)

    assert stats.attempted == 0 or stats.enriched == 0
    assert client.calls == 0
