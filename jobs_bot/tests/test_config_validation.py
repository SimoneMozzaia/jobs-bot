from __future__ import annotations

import pytest

from jobs_bot.config import Settings, validate_settings


def test_validate_settings_rejects_invalid_ranges() -> None:
    s = Settings(
        notion_token="x",
        notion_version="2025-09-03",
        notion_data_source_id="y",
        mysql_host="localhost",
        mysql_port=70000,  # invalid
        mysql_db="db",
        mysql_user="u",
        mysql_password="p",
        max_fetch_per_run=0,  # invalid
        sync_to_notion=2,  # invalid
    )

    with pytest.raises(RuntimeError):
        validate_settings(s)


def test_validate_settings_accepts_valid_config() -> None:
    s = Settings(
        notion_token="x",
        notion_version="2025-09-03",
        notion_data_source_id="y",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_db="db",
        mysql_user="u",
        mysql_password="p",
        sync_limit=50,
        fit_min=60,
        max_calls_per_day=50,
        request_timeout_s=20,
        greenhouse_per_page=100,
        greenhouse_max_pages=10,
        ingest_per_source_limit=0,
        max_fetch_per_run=50,
        max_new_jobs_per_day=200,
        sync_to_notion=1,
    )

    validate_settings(s)
