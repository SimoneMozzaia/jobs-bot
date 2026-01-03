from __future__ import annotations

from jobs_bot.ingest_ats import ingest_all_sources
from jobs_bot.models import Job, Source


def test_ingest_all_sources_ingests_jobs(sqlite_session, monkeypatch):
    lever = Source(
        ats_type="lever",
        company_slug="acme",
        company_name="ACME",
        api_base="https://api.lever.co/v0/postings/acme",
        is_active=1,
        discovered_via="manual",
    )
    gh = Source(
        ats_type="greenhouse",
        company_slug="stripe",
        company_name="Stripe",
        api_base="https://boards-api.greenhouse.io/v1/boards/stripe",
        is_active=1,
        discovered_via="manual",
    )
    sqlite_session.add_all([lever, gh])
    sqlite_session.commit()

    def _fake_lever(_api_base: str, timeout_s: int = 20):
        return [
            {"ats_job_id": "l1", "title": "A", "url": "u1", "raw_json": {}, "raw_text": ""},
            {"ats_job_id": "l2", "title": "B", "url": "u2", "raw_json": {}, "raw_text": ""},
        ]

    def _fake_gh_page(_api_base: str, page: int, timeout_s: int = 20, per_page: int = 100):
        if page == 1:
            return [{"ats_job_id": "g1", "title": "C", "url": "u3", "raw_json": {}, "raw_text": ""}]
        return []

    monkeypatch.setattr("jobs_bot.ingest_ats.fetch_lever_postings", _fake_lever)
    monkeypatch.setattr("jobs_bot.ingest_ats.fetch_greenhouse_jobs_page", _fake_gh_page)

    ok, created = ingest_all_sources(
        sqlite_session,
        max_calls_per_day=100,
        max_new_jobs_per_day=100,
        max_fetch_per_run=50,
        request_timeout_s=5,
        greenhouse_per_page=100,
        greenhouse_max_pages=10,
        per_source_limit=None,
    )

    assert ok == 2
    assert created == 3
    assert sqlite_session.query(Job).count() == 3


def test_ingest_respects_max_fetch_per_run(sqlite_session, monkeypatch):
    lever = Source(
        ats_type="lever",
        company_slug="acme",
        company_name="ACME",
        api_base="https://api.lever.co/v0/postings/acme",
        is_active=1,
        discovered_via="manual",
    )
    sqlite_session.add(lever)
    sqlite_session.commit()

    def _fake_lever(_api_base: str, timeout_s: int = 20):
        return [
            {"ats_job_id": "1", "title": "A", "url": "u1", "raw_json": {}, "raw_text": ""},
            {"ats_job_id": "2", "title": "B", "url": "u2", "raw_json": {}, "raw_text": ""},
            {"ats_job_id": "3", "title": "C", "url": "u3", "raw_json": {}, "raw_text": ""},
        ]

    monkeypatch.setattr("jobs_bot.ingest_ats.fetch_lever_postings", _fake_lever)

    ok, created = ingest_all_sources(
        sqlite_session,
        max_calls_per_day=100,
        max_new_jobs_per_day=100,
        max_fetch_per_run=2,
        request_timeout_s=5,
        greenhouse_per_page=100,
        greenhouse_max_pages=1,
        per_source_limit=None,
    )

    assert ok == 1
    assert created == 2
    assert sqlite_session.query(Job).count() == 2
