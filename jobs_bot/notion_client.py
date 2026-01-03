from __future__ import annotations

import requests


class NotionError(RuntimeError):
    pass


class NotionClient:
    def __init__(self, *, token: str, version: str, data_source_id: str, timeout_s: int = 20):
        self._token = token
        self._version = version
        self._data_source_id = data_source_id
        self._timeout_s = timeout_s

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Notion-Version": self._version,
                "Content-Type": "application/json",
            }
        )

    def _raise_for_error(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            raise NotionError(f"Notion error {resp.status_code}: {resp.text}")

    def create_page(self, properties: dict) -> str:
        payload = {
            "parent": {"type": "data_source_id", "data_source_id": self._data_source_id},
            "properties": properties,
        }
        resp = self._session.post(
            "https://api.notion.com/v1/pages",
            json=payload,
            timeout=self._timeout_s,
        )
        self._raise_for_error(resp)
        data = resp.json()
        return data["id"]

    def update_page(self, page_id: str, properties: dict) -> None:
        payload = {"properties": properties}
        resp = self._session.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            json=payload,
            timeout=self._timeout_s,
        )
        self._raise_for_error(resp)

    def query_by_job_uid(self, job_uid: str) -> str | None:
        payload = {
            "filter": {"property": "Job UID", "rich_text": {"equals": job_uid}},
            "page_size": 1,
        }
        resp = self._session.post(
            f"https://api.notion.com/v1/data_sources/{self._data_source_id}/query",
            json=payload,
            timeout=self._timeout_s,
        )
        self._raise_for_error(resp)
        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None
        return results[0]["id"]
