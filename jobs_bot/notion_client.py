from __future__ import annotations

import datetime as dt
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class NotionError(RuntimeError):
    pass


class NotionClient:
    def __init__(self, token: str, version: str, data_source_id: str, timeout_s: int = 30):
        self.token = token
        self.version = version
        self.data_source_id = data_source_id
        self.timeout_s = timeout_s
        self.base = "https://api.notion.com/v1"

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": self.version,
            "Content-Type": "application/json",
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(NotionError),
    )
    def _request(self, method: str, path: str, json: dict | None = None) -> dict:
        url = f"{self.base}{path}"
        r = requests.request(method, url, headers=self._headers(), json=json, timeout=self.timeout_s)

        # Retry on rate limiting / transient errors
        if r.status_code in (429, 500, 502, 503, 504):
            raise NotionError(f"Transient Notion error {r.status_code}: {r.text[:400]}")

        if not r.ok:
            raise NotionError(f"Notion error {r.status_code}: {r.text[:800]}")

        return r.json()

    def query_by_job_uid(self, job_uid: str) -> str | None:
        payload = {
            "filter": {
                "property": "Job UID",
                "rich_text": {"equals": job_uid},
            },
            "page_size": 1,
        }
        data = self._request("POST", f"/data_sources/{self.data_source_id}/query", json=payload)
        results = data.get("results", [])
        if not results:
            return None
        return results[0]["id"]

    def create_page(self, properties: dict) -> str:
        payload = {
            "parent": {"type": "data_source_id", "data_source_id": self.data_source_id},
            "properties": properties,
        }
        data = self._request("POST", "/pages", json=payload)
        return data["id"]

    def update_page(self, page_id: str, properties: dict) -> None:
        payload = {"properties": properties}
        self._request("PATCH", f"/pages/{page_id}", json=payload)

    @staticmethod
    def _date_only(d: dt.datetime | dt.date | None) -> str | None:
        if d is None:
            return None
        if isinstance(d, dt.datetime):
            return d.date().isoformat()
        return d.isoformat()

