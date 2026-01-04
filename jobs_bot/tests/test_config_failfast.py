import pytest

from jobs_bot.config import Settings, validate_settings


def _base_settings(**overrides):
    s = Settings(
        notion_token="",
        notion_version="2025-09-03",
        notion_data_source_id="",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_db="jobs",
        mysql_user="u",
        mysql_password="p",
        sync_limit=50,
        fit_min=60,
        max_calls_per_day=50,
        request_timeout_s=20,
        greenhouse_per_page=100,
        greenhouse_max_pages=50,
        ingest_per_source_limit=0,
        max_fetch_per_run=50,
        max_new_jobs_per_day=200,
        sync_to_notion=0,
    )
    data = {**s.__dict__, **overrides}
    return Settings(**data)


def test_validate_settings_allows_no_notion_when_sync_disabled():
    s = _base_settings(sync_to_notion=0, notion_token="", notion_data_source_id="")
    validate_settings(s)  # should not raise


def test_validate_settings_requires_notion_when_sync_enabled():
    s = _base_settings(sync_to_notion=1, notion_token="", notion_data_source_id="")
    with pytest.raises(RuntimeError):
        validate_settings(s)
