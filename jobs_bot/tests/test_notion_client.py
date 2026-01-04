from __future__ import annotations

from typing import Any

from jobs_bot.notion_client import NotionClient


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []
        self._next: list[_FakeResponse] = []

    def queue(self, resp: _FakeResponse) -> None:
        self._next.append(resp)

    def post(self, url: str, json: dict[str, Any], timeout: int) -> _FakeResponse:
        self.calls.append({"method": "POST", "url": url, "json": json, "timeout": timeout})
        return self._next.pop(0)

    def patch(self, url: str, json: dict[str, Any], timeout: int) -> _FakeResponse:
        self.calls.append({"method": "PATCH", "url": url, "json": json, "timeout": timeout})
        return self._next.pop(0)


def test_query_page_id_includes_profile_filter(monkeypatch):
    client = NotionClient(token="t", version="v", data_source_id="ds", timeout_s=5)
    fake = _FakeSession()
    fake.queue(_FakeResponse(200, {"results": [{"id": "page1"}]}))
    client._session = fake  # type: ignore[attr-defined]

    page_id = client.query_page_id(job_uid="abc", profile_id="p1")
    assert page_id == "page1"

    call = fake.calls[0]
    assert call["url"].endswith("/data_sources/ds/query")
    flt = call["json"]["filter"]["and"]
    assert {"property": "Job UID", "rich_text": {"equals": "abc"}} in flt
    assert {"property": "Profile", "rich_text": {"equals": "p1"}} in flt


def test_query_page_id_without_profile(monkeypatch):
    client = NotionClient(token="t", version="v", data_source_id="ds", timeout_s=5)
    fake = _FakeSession()
    fake.queue(_FakeResponse(200, {"results": []}))
    client._session = fake  # type: ignore[attr-defined]

    page_id = client.query_page_id(job_uid="abc", profile_id=None)
    assert page_id is None

    flt = fake.calls[0]["json"]["filter"]["and"]
    assert flt == [{"property": "Job UID", "rich_text": {"equals": "abc"}}]
