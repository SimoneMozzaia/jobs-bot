from __future__ import annotations

from jobs_bot.api_usage import can_consume_call, can_create_new_job


def test_can_consume_call_respects_daily_cap(sqlite_session):
    assert can_consume_call(sqlite_session, "lever", max_per_day=2) is True
    assert can_consume_call(sqlite_session, "lever", max_per_day=2) is True
    assert can_consume_call(sqlite_session, "lever", max_per_day=2) is False


def test_can_create_new_job_respects_daily_cap(sqlite_session):
    assert can_create_new_job(sqlite_session, max_new_per_day=1) is True
    assert can_create_new_job(sqlite_session, max_new_per_day=1) is False
