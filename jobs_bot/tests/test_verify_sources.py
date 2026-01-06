from __future__ import annotations

import responses

from jobs_bot.models import Source
from jobs_bot.verify_sources import verify_and_promote_sources


@responses.activate
def test_verify_and_promote_greenhouse(sqlite_session) -> None:
    src = Source(
        ats_type="greenhouse",
        company_slug="acme",
        api_base="https://boards-api.greenhouse.io/v1/boards/acme",
        company_name="Acme",
        discovered_via="cmc:US",
        is_active=0,
        verified_at=None,
        last_ok_at=None,
        last_error=None,
    )
    sqlite_session.add(src)
    sqlite_session.commit()

    responses.add(
        responses.GET,
        "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
        json={"jobs": []},
        status=200,
    )

    ok, failed = verify_and_promote_sources(
        sqlite_session, [src], timeout_s=10, delay_s=0
    )
    assert ok == 1
    assert failed == 0

    sqlite_session.refresh(src)
    assert src.is_active == 1
    assert src.verified_at is not None
    assert src.last_ok_at is not None
    assert src.last_error is None


@responses.activate
def test_verify_and_promote_failure_keeps_inactive(sqlite_session) -> None:
    src = Source(
        ats_type="workable",
        company_slug="acme",
        api_base="https://apply.workable.com/api/v1/accounts/acme/jobs",
        company_name="Acme",
        discovered_via="cmc:EU",
        is_active=0,
        verified_at=None,
        last_ok_at=None,
        last_error=None,
    )
    sqlite_session.add(src)
    sqlite_session.commit()

    responses.add(
        responses.GET,
        "https://apply.workable.com/api/v1/accounts/acme/jobs",
        status=500,
        json={"error": "boom"},
    )

    ok, failed = verify_and_promote_sources(
        sqlite_session, [src], timeout_s=10, delay_s=0
    )
    assert ok == 0
    assert failed == 1

    sqlite_session.refresh(src)
    assert src.is_active == 0
    assert src.verified_at is None
    assert src.last_ok_at is None
    assert src.last_error is not None
