from __future__ import annotations

import datetime as dt
import re
from typing import Any
import requests


def _truncate(s: str | None, n: int) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    return s[:n] if len(s) > n else s


_salary_re = re.compile(
    r"(?i)\b(\$|€|£)\s?\d[\d,\. ]{2,}\s?(?:-\s?(\$|€|£)?\s?\d[\d,\. ]{2,})?"
)


def extract_salary_text(text: str | None) -> str | None:
    if not text:
        return None
    m = _salary_re.search(text)
    if not m:
        return None
    return _truncate(m.group(0).strip(), 255)


def _ensure_json(resp: requests.Response) -> Any:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" in ctype:
        return resp.json()

    # Fallback: some endpoints lie; try parse if it looks like JSON
    t = (resp.text or "").lstrip()
    if t.startswith("{") or t.startswith("["):
        return resp.json()

    raise ValueError(f"Non-JSON response (content-type={ctype})")


def fetch_lever_postings(api_base: str, timeout_s: int = 20) -> list[dict]:
    # Lever returns HTML unless mode=json
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

        # createdAt is ms epoch (often)
        posted_at = None
        ca = p.get("createdAt")
        if isinstance(ca, (int, float)):
            posted_at = dt.datetime.fromtimestamp(ca / 1000.0, tz=dt.UTC).replace(tzinfo=None)

        location_raw = None
        cats = p.get("categories") or {}
        if isinstance(cats, dict):
            location_raw = cats.get("location")

        desc = p.get("descriptionPlain") or p.get("description") or ""
        if isinstance(desc, str):
            raw_text = desc.strip()
        else:
            raw_text = ""

        out.append(
            {
                "ats_job_id": job_id,
                "title": _truncate(title, 512) or "Untitled",
                "url": _truncate(hosted_url, 1024) or "",
                "location_raw": _truncate(location_raw, 512),
                "workplace_raw": _truncate("Remote" if (location_raw and "remote" in str(location_raw).lower()) else None, 128),
                "posted_at": posted_at,
                "raw_json": p,
                "raw_text": raw_text,
                "salary_text": extract_salary_text(raw_text),
            }
        )

    return out


def fetch_greenhouse_jobs(api_base: str, timeout_s: int = 20, max_pages: int = 50, per_page: int = 100) -> list[dict]:
    # api_base example: https://boards-api.greenhouse.io/v1/boards/stripe
    base = api_base.rstrip("/")
    url = f"{base}/jobs"

    out: list[dict] = []
    for page in range(1, max_pages + 1):
        resp = requests.get(url, params={"page": page, "per_page": per_page}, timeout=timeout_s)
        resp.raise_for_status()
        data = _ensure_json(resp)

        jobs = data.get("jobs") if isinstance(data, dict) else None
        if not jobs:
            break

        if not isinstance(jobs, list):
            raise ValueError("Greenhouse jobs field is not a list")

        for j in jobs:
            if not isinstance(j, dict):
                continue
            job_id = j.get("id")
            if job_id is None:
                continue

            title = str(j.get("title") or "").strip()
            abs_url = str(j.get("absolute_url") or "").strip()

            loc = j.get("location") or {}
            location_raw = None
            if isinstance(loc, dict):
                location_raw = loc.get("name")

            # Some payloads have updated_at; treat as posted_at fallback
            posted_at = None
            ua = j.get("updated_at")
            if isinstance(ua, str) and ua:
                try:
                    # e.g. 2025-01-01T00:00:00Z
                    posted_at = dt.datetime.fromisoformat(ua.replace("Z", "+00:00")).astimezone(dt.UTC).replace(tzinfo=None)
                except Exception:
                    posted_at = None

            out.append(
                {
                    "ats_job_id": str(job_id),
                    "title": _truncate(title, 512) or "Untitled",
                    "url": _truncate(abs_url, 1024) or "",
                    "location_raw": _truncate(location_raw, 512),
                    "workplace_raw": _truncate("Remote" if (location_raw and "remote" in str(location_raw).lower()) else None, 128),
                    "posted_at": posted_at,
                    "raw_json": j,
                    "raw_text": "",  # v0: no detail call to keep API calls low
                    "salary_text": None,
                }
            )

    return out

def fetch_greenhouse_jobs_page(
    api_base: str,
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
                "raw_text": "",  # no detail call
                "salary_text": None,
            }
        )

    return out
