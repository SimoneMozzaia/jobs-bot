from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from jobs_bot.models import Base

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture()
def sqlite_session() -> Session:
    """SQLite in-memory DB with helper tables for api usage caps."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE api_daily_usage (
              day DATE NOT NULL,
              ats_type TEXT NOT NULL,
              calls INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (day, ats_type)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE job_daily_new (
              day DATE NOT NULL PRIMARY KEY,
              created INTEGER NOT NULL DEFAULT 0
            )
            """
        )

    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@dataclass
class FakeNotionClient:
    pages: dict[str, dict[str, Any]] = field(default_factory=dict)
    job_uid_to_page: dict[str, str] = field(default_factory=dict)
    created_payloads: list[dict[str, Any]] = field(default_factory=list)
    updated_payloads: list[dict[str, Any]] = field(default_factory=list)

    def create_page(self, properties: dict) -> str:
        page_id = str(uuid.uuid4())
        self.pages[page_id] = properties
        self.created_payloads.append(properties)

        job_uid_items = (properties.get("Job UID") or {}).get("rich_text") or []
        if job_uid_items:
            job_uid = job_uid_items[0]["text"]["content"]
            self.job_uid_to_page[job_uid] = page_id

        return page_id

    def update_page(self, page_id: str, properties: dict) -> None:
        self.updated_payloads.append(properties)
        self.pages[page_id] = {**self.pages.get(page_id, {}), **properties}

    def query_by_job_uid(self, job_uid: str) -> str | None:
        return self.job_uid_to_page.get(job_uid)


@pytest.fixture()
def fake_notion() -> FakeNotionClient:
    return FakeNotionClient()
