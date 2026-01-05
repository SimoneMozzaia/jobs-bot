import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from jobs_bot.models import Base


@pytest.fixture()
def sqlite_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture()
def sqlite_session(sqlite_engine):
    with Session(sqlite_engine) as session:
        yield session


@dataclass
class FakeNotionClient:
    pages: dict[str, dict[str, Any]] = field(default_factory=dict)
    job_uid_to_page: dict[str, str] = field(default_factory=dict)
    job_uid_profile_to_page: dict[tuple[str, str], str] = field(default_factory=dict)
    created_payloads: list[dict[str, Any]] = field(default_factory=list)
    updated_payloads: list[dict[str, Any]] = field(default_factory=list)

    def _read_rich_text(self, properties: dict[str, Any], key: str) -> str | None:
        items = (properties.get(key) or {}).get("rich_text") or []
        if not items:
            return None
        return items[0]["text"]["content"]

    def create_page(self, properties: dict) -> str:
        page_id = str(uuid.uuid4())
        self.pages[page_id] = properties
        self.created_payloads.append(properties)

        job_uid = self._read_rich_text(properties, "Job UID")
        profile_id = self._read_rich_text(properties, "Profile") or ""

        if job_uid:
            self.job_uid_to_page[job_uid] = page_id
            self.job_uid_profile_to_page[(job_uid, profile_id)] = page_id

        return page_id

    def update_page(self, page_id: str, properties: dict) -> None:
        self.updated_payloads.append(properties)
        self.pages[page_id] = {**self.pages.get(page_id, {}), **properties}

        job_uid = self._read_rich_text(self.pages[page_id], "Job UID")
        profile_id = self._read_rich_text(self.pages[page_id], "Profile") or ""
        if job_uid:
            self.job_uid_to_page[job_uid] = page_id
            self.job_uid_profile_to_page[(job_uid, profile_id)] = page_id

    def query_by_job_uid(self, job_uid: str) -> str | None:
        return self.job_uid_to_page.get(job_uid)

    def query_page_id(self, *, job_uid: str, profile_id: str) -> str | None:
        return self.job_uid_profile_to_page.get((job_uid, profile_id))


@pytest.fixture()
def fake_notion() -> FakeNotionClient:
    return FakeNotionClient()
