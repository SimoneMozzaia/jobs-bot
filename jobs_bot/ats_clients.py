from __future__ import annotations

import datetime as dt
import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urlparse

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
    url = f"{base}/jobs"

    # Keep the request URL stable by omitting default pagination params.
    # We always request content to enable downstream parsing (salary, etc.).
    params: dict[str, Any] = {"content": "true"}
    if page and page > 1:
        params["page"] = int(page)
    if per_page and per_page != 100:
        params["per_page"] = int(per_page)

    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or {}
    jobs = data.get("jobs") or []

    out: list[dict[str, Any]] = []
    for item in jobs:
        location = item.get("location") or {}
        location_raw = location.get("name") if isinstance(location, dict) else None

        raw_text = item.get("content") or ""
        salary_text: str | None = None
        metadata = item.get("metadata") or []
        if isinstance(metadata, list):
            for m in metadata:
                if not isinstance(m, dict):
                    continue
                name = (m.get("name") or "").strip().lower()
                value = m.get("value")
                if not isinstance(value, str):
                    continue
                if name == "salary":
                    salary_text = _truncate(value, 255)
                    break

        # Fallback to regex extraction from the content.
        salary_text = salary_text or extract_salary_text(raw_text)

        out.append(
            {
                "ats_job_id": str(item.get("id") or ""),
                "title": _truncate(item.get("title"), 512) or "Untitled",
                "url": _truncate(item.get("absolute_url"), 1024) or "",
                "posted_at": None,
                "location_raw": _truncate(location_raw, 512) if isinstance(location_raw, str) else None,
                "workplace_raw": None,
                "raw_text": raw_text,
                "salary_text": salary_text,
                "raw_json": item,
            }
        )
    return out


def fetch_workday_jobs_page(
    api_base: str,
    *,
    offset: int,
    limit: int,
    timeout_s: int = 20,
) -> list[dict[str, Any]]:
    """Fetch a page of Workday job postings from the public cxs endpoint."""
    base = api_base.rstrip("/")

    payload = {"limit": int(limit), "offset": int(offset)}
    resp = requests.post(base, json=payload, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or {}

    postings = data.get("jobPostings") or []
    if not isinstance(postings, list):
        return []

    parsed = urlparse(base)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    # Typical cxs path: /wday/cxs/<tenant>/<site>/jobs
    parts = [p for p in parsed.path.split("/") if p]
    site = parts[4] if len(parts) >= 5 and parts[:2] == ["wday", "cxs"] else ""
    public_base = f"{origin}/{site}" if site else origin

    out: list[dict[str, Any]] = []
    for item in postings:
        if not isinstance(item, dict):
            continue

        bullet_fields = item.get("bulletFields") or []
        ats_job_id = None
        if isinstance(bullet_fields, list) and bullet_fields:
            first = bullet_fields[0]
            if isinstance(first, str) and first.strip():
                ats_job_id = first.strip()

        if not ats_job_id:
            # Best-effort fallback (not guaranteed): extract from externalPath.
            ext = item.get("externalPath")
            if isinstance(ext, str) and "_" in ext:
                ats_job_id = ext.split("_")[-1]

        ats_job_id = str(ats_job_id or "")

        external_path = item.get("externalPath")
        job_url = ""
        if isinstance(external_path, str) and external_path.strip():
            external_path = external_path.strip()
            if not external_path.startswith("/"):
                external_path = f"/{external_path}"
            job_url = f"{public_base}{external_path}"

        location_raw = item.get("locationsText")
        out.append(
            {
                "ats_job_id": ats_job_id,
                "title": _truncate(item.get("title"), 512) or "Untitled",
                "url": _truncate(job_url, 1024) or "",
                "posted_at": None,
                "location_raw": _truncate(location_raw, 512) if isinstance(location_raw, str) else None,
                "workplace_raw": None,
                "raw_text": None,
                "salary_text": None,
                "raw_json": item,
            }
        )

    return out


def fetch_workable_jobs(api_base: str, *, timeout_s: int = 20) -> list[dict[str, Any]]:
    base = api_base.rstrip("/")

    resp = requests.get(base, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json() or {}
    jobs = data.get("jobs") or []
    if not isinstance(jobs, list):
        return []

    out: list[dict[str, Any]] = []
    for item in jobs:
        if not isinstance(item, dict):
            continue

        raw_html = item.get("description")
        raw_text = _strip_html(raw_html) if isinstance(raw_html, str) else ""

        location_raw = item.get("location")
        out.append(
            {
                "ats_job_id": str(item.get("shortcode") or ""),
                "title": _truncate(item.get("title"), 512) or "Untitled",
                "url": _truncate(item.get("url"), 1024) or "",
                "posted_at": None,
                "location_raw": _truncate(location_raw, 512) if isinstance(location_raw, str) else None,
                "workplace_raw": None,
                "raw_text": raw_text,
                "salary_text": extract_salary_text(raw_text),
                "raw_json": item,
            }
        )

    return out


def fetch_successfactors_postings(api_base: str, *, timeout_s: int = 20) -> list[dict[str, Any]]:
    base = api_base.rstrip("/")

    resp = requests.get(base, timeout=timeout_s)
    resp.raise_for_status()
    xml_text = resp.text or ""
    if not xml_text.strip():
        return []

    root = ET.fromstring(xml_text)
    out: list[dict[str, Any]] = []

    for jp in root.findall(".//jobPosting"):
        job_req_id = (jp.findtext("jobReqId") or "").strip()
        title = (jp.findtext("jobTitle") or "").strip()
        location = (jp.findtext("jobLocation") or "").strip()
        apply_url = (jp.findtext("applyUrl") or "").strip()
        desc = jp.findtext("jobDescription") or ""
        raw_text = _strip_html(desc)

        out.append(
            {
                "ats_job_id": job_req_id,
                "title": _truncate(title, 512) or "Untitled",
                "url": _truncate(apply_url, 1024) or "",
                "posted_at": None,
                "location_raw": _truncate(location, 512) or None,
                "workplace_raw": None,
                "raw_text": raw_text,
                "salary_text": extract_salary_text(raw_text),
                "raw_json": {
                    "jobReqId": job_req_id,
                    "jobTitle": title,
                    "jobLocation": location,
                    "applyUrl": apply_url,
                },
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
