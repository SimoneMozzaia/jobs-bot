from __future__ import annotations

import responses

from jobs_bot.ats_clients import fetch_greenhouse_jobs_page, fetch_lever_postings


@responses.activate
def test_fetch_lever_postings_appends_mode_json_and_parses():
    base = "https://api.lever.co/v0/postings/testco"
    url = f"{base}?mode=json"

    responses.add(
        responses.GET,
        url,
        json=[
            {
                "id": "abc123",
                "text": "Engineer",
                "hostedUrl": "https://jobs.lever.co/testco/abc123",
                "createdAt": 1700000000000,
                "categories": {"location": "Remote"},
                "descriptionPlain": "Salary € 80,000 - € 100,000",
            }
        ],
        status=200,
        content_type="application/json",
    )

    jobs = fetch_lever_postings(base, timeout_s=5)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "abc123"
    assert jobs[0]["salary_text"] == "€ 80,000 - € 100,000"


@responses.activate
def test_fetch_greenhouse_jobs_page_parses_jobs_list():
    api_base = "https://boards-api.greenhouse.io/v1/boards/acme"
    url = f"{api_base}/jobs"

    responses.add(
        responses.GET,
        url,
        json={
            "jobs": [
                {
                    "id": 42,
                    "title": "Data Engineer",
                    "absolute_url": "https://boards.greenhouse.io/acme/jobs/42",
                    "location": {"name": "Remote"},
                    "updated_at": "2026-01-03T00:00:00Z",
                }
            ]
        },
        status=200,
        content_type="application/json",
    )

    jobs = fetch_greenhouse_jobs_page(api_base, page=1, timeout_s=5, per_page=100)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "42"
    assert jobs[0]["title"] == "Data Engineer"
