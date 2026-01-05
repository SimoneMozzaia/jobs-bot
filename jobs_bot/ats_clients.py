from __future__ import annotations

import datetime as dt
import re
from typing import Any

import requests


def _truncate(value: str | None, max_len: int) -> str | None:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    return v[:max_len]


# NOTE:
# Do NOT use a leading "\b" (word boundary) here.
# Currency symbols like "€" are not word-characters, and a boundary won't exist
# between whitespace and a non-word symbol. That would cause valid salary
# strings like "Salary € 80,000 - € 100,000" to not match.
_salary_re = re.compile(
    r"(?i)(\$|€|£)\s*\d[\d,\. ]{1,}\s*(?:-\s*(\$|€|£)?\s*\d[\d,\. ]{1,})?"
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(value: str) -> str:
    txt = (value or "").replace("\r", "").strip()
    if not txt:
        return ""
    txt = _HTML_TAG_RE.sub(" ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def extract_salary_text(text: str | None) -> str | None:
    if not text:
        return None
    m = _salary_re.search(text)
    if not m:
        return None
    return _truncate(m.group(0), 255)


def _ms_to_dt_utc(ms: int | None) -> dt.datetime | None:
    if not ms:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.UTC).replace(tzinfo=None)


def _ensure_json(resp: requests.Response) -> Any:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" in ctype:
        return resp.json()

    t = (resp.text or "").lstrip()
    if t.startswith("{") or t.startswith("["):
        return resp.json()

    raise ValueError(f"Non-JSON response (content-type={ctype})")


def fetch_lever_postings(api_base: str, *, timeout_s: int = 20) -> list[dict[str, Any]]:
    base = api_base.rstrip("/")
    url = f"{base}?mode=json"

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or []

    out: list[dict[str, Any]] = []
    for item in data:
        raw_text = item.get("descriptionPlain") or ""
        salary_text = extract_salary_text(raw_text)

        categories = item.get("categories") or {}
        location_raw = categories.get("location")
        workplace_raw = None
        if isinstance(location_raw, str):
            loc_lower = location_raw.lower()
            if "remote" in loc_lower:
                workplace_raw = "Remote"

        out.append(
            {
                "ats_job_id": str(item.get("id") or ""),
                "title": _truncate(item.get("text"), 512) or "Untitled",
                "url": _truncate(item.get("hostedUrl"), 1024) or "",
                "posted_at": _ms_to_dt_utc(item.get("createdAt")),
                "location_raw": _truncate(location_raw, 512) if isinstance(location_raw, str) else None,
                "workplace_raw": _truncate(workplace_raw, 128),
                "raw_text": raw_text,
                "salary_text": salary_text,
                "raw_json": item,
            }
        )

    return out


def fetch_greenhouse_jobs_page(
    api_base: str,
    *,
    page: int,
    timeout_s: int = 20,
    per_page: int = 100,
) -> list[dict[str, Any]]:
    base = api_base.rstrip("/")
    url = f"{base}/jobs?page={page}&per_page={per_page}"

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or {}
    jobs = data.get("jobs") or []

    out: list[dict[str, Any]] = []
    for item in jobs:
        location = item.get("location") or {}
        location_raw = location.get("name") if isinstance(location, dict) else None

        out.append(
            {
                "ats_job_id": str(item.get("id") or ""),
                "title": _truncate(item.get("title"), 512) or "Untitled",
                "url": _truncate(item.get("absolute_url"), 1024) or "",
                "posted_at": None,
                "location_raw": _truncate(location_raw, 512) if isinstance(location_raw, str) else None,
                "workplace_raw": None,
                "raw_text": None,
                "salary_text": None,
                "raw_json": item,
            }
        )
    return out


def fetch_greenhouse_job_detail(
    api_base: str,
    *,
    ats_job_id: str,
    timeout_s: int = 20,
) -> dict[str, Any]:
    base = api_base.rstrip("/")
    url = f"{base}/jobs/{ats_job_id}"

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or {}

    raw_text = data.get("content") or ""
    salary_text = extract_salary_text(raw_text)

    return {"raw_text": raw_text, "salary_text": salary_text}
