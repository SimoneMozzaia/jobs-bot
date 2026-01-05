import responses

from jobs_bot.ats_clients import (
    fetch_greenhouse_jobs_page,
    fetch_lever_postings,
    fetch_successfactors_postings,
    fetch_workable_jobs,
    fetch_workday_jobs_page,
)


@responses.activate
def test_fetch_lever_postings_parses():
    api_base = "https://api.lever.co/v0/postings/acme"
    responses.add(
        responses.GET,
        api_base,
        json=[
            {"id": "1", "text": "Engineer", "hostedUrl": "u1", "descriptionPlain": "desc"},
        ],
        status=200,
    )
    jobs = fetch_lever_postings(api_base)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "1"
    assert jobs[0]["title"] == "Engineer"
    assert jobs[0]["url"] == "u1"
    assert "desc" in jobs[0]["raw_text"]


@responses.activate
def test_fetch_greenhouse_jobs_page_parses():
    api_base = "https://boards-api.greenhouse.io/v1/boards/acme"
    responses.add(
        responses.GET,
        f"{api_base}/jobs?content=true&page=1&per_page=100",
        json={
            "jobs": [
                {
                    "id": 10,
                    "title": "SWE",
                    "absolute_url": "u",
                    "location": {"name": "NY"},
                    "content": "Hello",
                    "metadata": [{"name": "Salary", "value": "$100k"}],
                }
            ]
        },
        status=200,
    )
    jobs = fetch_greenhouse_jobs_page(api_base, page=1, per_page=100)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "10"
    assert jobs[0]["salary_text"] == "$100k"
    assert jobs[0]["location_raw"] == "NY"


@responses.activate
def test_fetch_workday_jobs_page_parses():
    api_base = "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/jobs"
    responses.add(
        responses.POST,
        api_base,
        json={
            "jobPostings": [
                {
                    "title": "Senior Engineer",
                    "externalPath": "/job/Remote/Senior-Engineer_JR1954638",
                    "locationsText": "Remote",
                    "bulletFields": ["JR1954638"],
                }
            ],
            "total": 1,
        },
        status=200,
    )
    jobs = fetch_workday_jobs_page(api_base, offset=0, limit=20)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "JR1954638"
    assert jobs[0]["title"] == "Senior Engineer"
    assert jobs[0]["location_raw"] == "Remote"
    assert jobs[0]["url"].startswith("https://acme.wd5.myworkdayjobs.com/")


@responses.activate
def test_fetch_workable_jobs_parses():
    api_base = "https://www.workable.com/api/accounts/acme"
    responses.add(
        responses.GET,
        api_base,
        json={
            "jobs": [
                {
                    "shortcode": "ABC123",
                    "title": "Backend Engineer",
                    "location": "Remote",
                    "description": "<p>Python</p>",
                }
            ]
        },
        status=200,
    )
    jobs = fetch_workable_jobs(api_base)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "ABC123"
    assert jobs[0]["title"] == "Backend Engineer"
    assert jobs[0]["location_raw"] == "Remote"
    assert "Python" in (jobs[0]["raw_text"] or "")


@responses.activate
def test_fetch_successfactors_postings_parses():
    api_base = "https://example.successfactors.com/jobPostingSummary"
    responses.add(
        responses.GET,
        api_base,
        body=(
            "<?xml version='1.0'?>"
            "<root>"
            "  <jobPosting>"
            "    <jobReqId>REQ-1</jobReqId>"
            "    <jobTitle>Data Engineer</jobTitle>"
            "    <jobLocation>Milano</jobLocation>"
            "    <applyUrl>https://example.com/apply/REQ-1</applyUrl>"
            "    <jobDescription><![CDATA[<p>SQL</p>]]></jobDescription>"
            "  </jobPosting>"
            "</root>"
        ),
        status=200,
        content_type="application/xml",
    )
    jobs = fetch_successfactors_postings(api_base)
    assert len(jobs) == 1
    assert jobs[0]["ats_job_id"] == "REQ-1"
    assert jobs[0]["title"] == "Data Engineer"
    assert jobs[0]["location_raw"] == "Milano"
    assert jobs[0]["url"] == "https://example.com/apply/REQ-1"
    assert "SQL" in (jobs[0]["raw_text"] or "")
