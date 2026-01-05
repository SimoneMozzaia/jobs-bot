import logging
from types import SimpleNamespace

import ingest_run
from jobs_bot.config import Settings
from jobs_bot.models import Profile


def test_ingest_run_pipeline_happy_path_orders_steps(sqlite_session, tmp_path, fake_notion, monkeypatch):
    calls: list[str] = []

    logger = logging.getLogger("test-pipeline")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    profiles_dir = tmp_path / "profiles"
    (profiles_dir / "default").mkdir(parents=True)
    cv_path = profiles_dir / "default" / "cv.docx"
    cv_path.write_bytes(b"dummy")

    settings = Settings(
        mysql_host="localhost",
        mysql_port=3306,
        mysql_db="db",
        mysql_user="u",
        mysql_password="p",
        notion_token="nt",
        notion_data_source_id="ds",
        notion_version="2025-09-03",
        sync_limit=10,
        fit_min=60,
        sync_to_notion=1,
        max_calls_per_day=0,
        max_new_jobs_per_day=0,
        max_fetch_per_run=10,
        request_timeout_s=5,
        greenhouse_per_page=100,
        greenhouse_max_pages=1,
        ingest_per_source_limit=0,
        profiles_dir=str(profiles_dir),
        profile_id="default",
        enrich_with_llm=1,
        enrich_limit=10,
        openai_api_key="sk-test",
        openai_model="gpt-4o-mini",
        openai_base_url="https://api.openai.com/v1",
    )

    def fake_ingest_all_sources(*args, **kwargs):
        calls.append("ingest")
        return 1, 1

    def fake_enrich_pending_jobs(*args, **kwargs):
        calls.append("enrich")
        return 1

    def fake_bootstrap_profile(session, *, profile_id, cv_path, now=None):
        calls.append("bootstrap")
        prof = Profile(
            profile_id=profile_id,
            cv_path=cv_path,
            cv_sha256="a" * 64,
            profile_json=None,
            profile_text="Python",
            analyzed_at=None,
            last_error=None,
        )
        session.merge(prof)
        session.commit()
        return prof, True

    def fake_compute_fit_scores_for_profile(*args, **kwargs):
        calls.append("score")
        return SimpleNamespace(attempted=1)

    def fake_sync_pending_jobs(*args, **kwargs):
        calls.append("notion")
        return 1

    monkeypatch.setattr(ingest_run, "ingest_all_sources", fake_ingest_all_sources)
    monkeypatch.setattr(ingest_run, "enrich_pending_jobs", fake_enrich_pending_jobs)
    monkeypatch.setattr(ingest_run, "bootstrap_profile", fake_bootstrap_profile)
    monkeypatch.setattr(ingest_run, "compute_fit_scores_for_profile", fake_compute_fit_scores_for_profile)
    monkeypatch.setattr(ingest_run, "sync_pending_jobs", fake_sync_pending_jobs)
    monkeypatch.setattr(ingest_run, "NotionClient", lambda **kwargs: fake_notion)

    results = ingest_run.run_pipeline(sqlite_session, settings=settings, logger=logger)

    assert calls == ["ingest", "enrich", "bootstrap", "score", "notion"]
    assert results["sources_ok"] == 1
    assert results["jobs_created"] == 1
    assert results["jobs_enriched"] == 1
    assert results["jobs_scored"] == 1
    assert results["notion_synced"] == 1
