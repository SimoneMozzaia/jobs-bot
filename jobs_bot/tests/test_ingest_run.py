from __future__ import annotations

from dataclasses import dataclass

import ingest_run
from jobs_bot.config import Settings


@dataclass
class _DummyStats:
    attempted: int
    enriched: int
    failed: int


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def test_ingest_run_llm_calls_enrich_with_client(monkeypatch, sqlite_session):
    """Regression: ingest_run must call enrich_pending_jobs with 'client'."""

    settings = Settings(
        notion_token="",
        notion_version="",
        notion_data_source_id="",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_db="jobs",
        mysql_user="user",
        mysql_password="pass",
        sync_to_notion=0,
        enrich_with_llm=1,
        enrich_limit=5,
        openai_api_key="test-key",
        openai_model="dummy-model",
        openai_base_url="https://example.invalid",
        profiles_dir="",
    )

    monkeypatch.setattr(ingest_run, "ingest_all_sources", lambda *a, **k: (0, 0))

    class DummyClient:
        pass

    monkeypatch.setattr(
        ingest_run,
        "OpenAIResponsesClient",
        lambda **kwargs: DummyClient(),
    )

    called = {"ok": False}

    def _fake_enrich_pending_jobs(session, *, client, limit):
        assert isinstance(client, DummyClient)
        assert limit == 5
        called["ok"] = True
        return _DummyStats(attempted=1, enriched=1, failed=0)

    monkeypatch.setattr(ingest_run, "enrich_pending_jobs", _fake_enrich_pending_jobs)

    results = ingest_run.run_pipeline(
        sqlite_session,
        settings=settings,
        logger=_DummyLogger(),
    )
    assert called["ok"] is True
    assert results["jobs_enriched"] == 1
