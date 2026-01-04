from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


class NotionError(RuntimeError):
    """Raised for Notion API errors (HTTP >= 400)."""


@dataclass(frozen=True)
class NotionConfig:
    token: str
    version: str
    data_source_id: str
    timeout_s: int = 20
    base_url: str = "https://api.notion.com/v1"


class NotionClient:
    """Small Notion API wrapper (data-sources query + pages create/update)."""

    def __init__(self, *, token: str, version: str, data_source_id: str, timeout_s: int = 20):
        self._cfg = NotionConfig(
            token=token,
            version=version,
            data_source_id=data_source_id,
            timeout_s=timeout_s,
        )

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._cfg.token}",
                "Notion-Version": self._cfg.version,
                "Content-Type": "application/json",
            }
        )

    @property
    def data_source_id(self) -> str:
        return self._cfg.data_source_id

    def _raise_for_error(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            raise NotionError(f"Notion error {resp.status_code}: {resp.text}")

    def query_page_id(self, *, job_uid: str, profile_id: str | None = None) -> str | None:
        """
        Find an existing page by Job UID and optional Profile.

        Multi-profile mode SHOULD pass profile_id to avoid collisions.
        """
        filters: list[dict[str, Any]] = [
            {"property": "Job UID", "rich_text": {"equals": job_uid}},
        ]
        if profile_id is not None:
            filters.append({"property": "Profile", "rich_text": {"equals": profile_id}})

        payload = {"filter": {"and": filters}, "page_size": 1}

        url = f"{self._cfg.base_url}/data_sources/{self._cfg.data_source_id}/query"
        resp = self._session.post(url, json=payload, timeout=self._cfg.timeout_s)
        self._raise_for_error(resp)

        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None
        return results[0].get("id")

    def create_page(self, *, properties: dict[str, Any], children: list[dict[str, Any]] | None = None) -> str:
        """
        Create a new page under the configured data source.

        Uses `parent.data_source_id`. If your workspace still requires `database_id`,
        this method automatically falls back.
        """
        url = f"{self._cfg.base_url}/pages"

        payload: dict[str, Any] = {
            "parent": {"data_source_id": self._cfg.data_source_id},
            "properties": properties,
        }
        if children:
            payload["children"] = children

        resp = self._session.post(url, json=payload, timeout=self._cfg.timeout_s)
        if resp.status_code == 400:
            # Fallback for older workspaces that still only accept database_id
            payload["parent"] = {"database_id": self._cfg.data_source_id}
            resp = self._session.post(url, json=payload, timeout=self._cfg.timeout_s)

        self._raise_for_error(resp)
        return resp.json()["id"]

    def update_page(self, *, page_id: str, properties: dict[str, Any]) -> None:
        url = f"{self._cfg.base_url}/pages/{page_id}"
        resp = self._session.patch(url, json={"properties": properties}, timeout=self._cfg.timeout_s)
        self._raise_for_error(resp)

    # Backward-compatible alias (single-profile legacy)
    def query_by_job_uid(self, job_uid: str) -> str | None:
        return self.query_page_id(job_uid=job_uid, profile_id=None)
