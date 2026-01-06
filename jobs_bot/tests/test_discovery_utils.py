from __future__ import annotations

import requests
import responses

from jobs_bot.ats_discovery import detect_ats_from_url_or_html
from jobs_bot.careers_discovery import discover_careers_url
from jobs_bot.companies_marketcap import parse_cmc_csv
from jobs_bot.source_discovery import parse_regions


def test_parse_regions_dedup_and_normalize() -> None:
    assert parse_regions("us, eu, ch, ca, uae, us") == [
        "US",
        "EU",
        "CH",
        "CANADA",
        "UAE",
    ]


def test_parse_cmc_csv_headerless() -> None:
    csv_text = "\n".join(
        [
            '"1","Apple","AAPL","3.0T","0.0%","United States"',
            '"2","Microsoft","MSFT","2.9T","0.0%","United States"',
        ]
    )
    companies = parse_cmc_csv(csv_text)
    assert len(companies) == 2
    assert companies[0].name == "Apple"
    assert companies[0].ticker == "AAPL"
    assert companies[0].country == "United States"
    assert companies[0].rank == 1


def test_detect_ats_greenhouse() -> None:
    html = '<a href="https://boards.greenhouse.io/acme">Jobs</a>'
    d = detect_ats_from_url_or_html(page_url="https://acme.com/careers", html=html)
    assert d is not None
    assert d.ats_type == "greenhouse"
    assert d.company_slug == "acme"
    assert d.api_base.endswith("/v1/boards/acme")


def test_detect_ats_workday() -> None:
    url = "https://acme.wd3.myworkdayjobs.com/en-US/Careers"
    d = detect_ats_from_url_or_html(page_url=url, html="")
    assert d is not None
    assert d.ats_type == "workday"
    assert d.company_slug == "acme/Careers"
    assert d.api_base == "https://acme.wd3.myworkdayjobs.com/wday/cxs/acme/Careers/jobs"


def test_detect_ats_successfactors() -> None:
    url = "https://career5.successfactors.eu/career?company=ACME"
    d = detect_ats_from_url_or_html(page_url=url, html="")
    assert d is not None
    assert d.ats_type == "successfactors"
    assert d.company_slug == "ACME"
    assert "career_ns=job_listing_summary" in d.api_base
    assert "resultType=XML" in d.api_base


@responses.activate
def test_discover_careers_url_finds_link() -> None:
    homepage = "https://example.com/"
    responses.add(
        responses.GET,
        homepage,
        body="<html><body><a href='/careers'>Careers</a></body></html>",
        status=200,
        content_type="text/html",
    )

    s = requests.Session()
    res = discover_careers_url(homepage, session=s, timeout_s=10)
    assert res.careers_url == "https://example.com/careers"
    assert res.reason == "link_found"
