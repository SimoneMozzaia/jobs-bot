from __future__ import annotations

import datetime as dt
import re
from typing import Any

import requests


def _truncate(value: str | None, max_len: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:max_len] if len(text) > max_len else text


_SALARY_RE = re.compile(
    r"(?i)(\$|€|£)\s?\d[\d,\. ]{2,}\s?(?:-\s?(\$|€|£)?\s?\d[\d,\. ]{2,})?"
)


def extract_salary_text(text: str | None) -> str | None:
    if not text:
        return None
    match = _SALARY_RE.search(text)
    if not match:
        return None
    return _truncate(match.group(0).strip(), 255)


def _ensure_json(resp: requests.Response) -> Any:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" in ctype:
        return resp.json()

    t = (resp.text or "").lstrip()
    if t.startswith("{") or t.startswith("["):
        return resp.json()

    raise ValueError(f"Non-JSON response (content-type={ctype})")


def fetch_lever_postings(api_base: str, *, timeout_s: int = 20) -> list[dict]:
    url = api_base
    if "mode=json" not in url:
        url = url + ("&" if "?" in url else "?") + "mode=json"

    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    data = _ensure_json(resp)
    if not isinstance(data, list):
        raise ValueError("Lever payload is not a list")

    out: list[dict] = []
    for p in data:
        if not isinstance(p, dict):
            continue
        job_id = str(p.get("id") or "").strip()
        if not job_id:
            continue

        title = str(p.get("text") or "").strip()
        hosted_url = str(p.get("hostedUrl") or "").strip()

        posted_at = None
        ca = p.get("createdAt")
        if isinstance(ca, (int, float)):
            posted_at = dt.datetime.fromtimestamp(ca / 1000.0, tz=dt.UTC).replace(tzinfo=None)

        location_raw = None
        cats = p.get("categories") or {}
        if isinstance(cats, dict):
            location_raw = cats.get("location")

        desc = p.get("descriptionPlain") or p.get("description") or ""
        raw_text = desc.strip() if isinstance(desc, str) else ""

        out.append(
            {
                "ats_job_id": job_id,
                "title": _truncate(title, 512) or "Untitled",
                "url": _truncate(hosted_url, 1024) or "",
                "location_raw": _truncate(location_raw, 512),
                "workplace_raw": _truncate(
                    "Remote" if (location_raw and "remote" in str(location_raw).lower()) else None, 128
                ),
                "posted_at": posted_at,
                "raw_json": p,
                "raw_text": raw_text,
                "salary_text": extract_salary_text(raw_text),
            }
        )

    return out


def fetch_greenhouse_jobs_page(
    api_base: str,
    *,
    page: int,
    timeout_s: int = 20,
    per_page: int = 100,
) -> list[dict]:
    base = api_base.rstrip("/")
    url = f"{base}/jobs"

    resp = requests.get(url, params={"page": page, "per_page": per_page}, timeout=timeout_s)
    resp.raise_for_status()
    data = _ensure_json(resp)

    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not jobs:
        return []
    if not isinstance(jobs, list):
        raise ValueError("Greenhouse jobs field is not a list")

    out: list[dict] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        job_id = j.get("id")
        if job_id is None:
            continue

        title = str(j.get("title") or "").strip()
        abs_url = str(j.get("absolute_url") or "").strip()

        loc = j.get("location") or {}
        location_raw = loc.get("name") if isinstance(loc, dict) else None

        posted_at = None
        ua = j.get("updated_at")
        if isinstance(ua, str) and ua:
            try:
                posted_at = (
                    dt.datetime.fromisoformat(ua.replace("Z", "+00:00"))
                    .astimezone(dt.UTC)
                    .replace(tzinfo=None)
                )
            except Exception:
                posted_at = None

        out.append(
            {
                "ats_job_id": str(job_id),
                "title": _truncate(title, 512) or "Untitled",
                "url": _truncate(abs_url, 1024) or "",
                "location_raw": _truncate(location_raw, 512),
                "workplace_raw": _truncate(
                    "Remote" if (location_raw and "remote" in str(location_raw).lower()) else None, 128
                ),
                "posted_at": posted_at,
                "raw_json": j,
                "raw_text": "",
                "salary_text": None,
            }
        )

    return out


def fetch_greenhouse_jobs(
    api_base: str,
    *,
    timeout_s: int = 20,
    max_pages: int = 50,
    per_page: int = 100,
) -> list[dict]:
    out: list[dict] = []
    for page in range(1, max_pages + 1):
        page_jobs = fetch_greenhouse_jobs_page(
            api_base,
            page=page,
            timeout_s=timeout_s,
            per_page=per_page,
        )
        if not page_jobs:
            break
        out.extend(page_jobs)
    return out
