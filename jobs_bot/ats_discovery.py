from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class DiscoveredAts:
    ats_type: str
    company_slug: str
    api_base: str
    evidence_url: str


_GH_RE = re.compile(r"https?://boards\.greenhouse\.io/([a-z0-9_-]+)/?", re.I)
_LEVER_RE = re.compile(r"https?://jobs\.lever\.co/([a-z0-9_-]+)/?", re.I)
_WORKABLE_RE = re.compile(r"https?://apply\.workable\.com/([a-z0-9_-]+)/?", re.I)
_WORKDAY_RE = re.compile(
    r"https?://([a-z0-9_-]+)\.(?:wd\d\.)?myworkdayjobs\.com/"
    r"([a-z]{2}-[a-z]{2})/([a-z0-9_-]+)/?",
    re.I,
)
_SFS_RE = re.compile(r"https?://[^\s\"']*successfactors[^\s\"']+", re.I)


def detect_ats_from_url_or_html(*, page_url: str, html: str) -> DiscoveredAts | None:
    """Detect ATS provider from a careers page (precision-first heuristics)."""

    text = " ".join([page_url or "", html or ""]).strip()
    if not text:
        return None

    m = _GH_RE.search(text)
    if m:
        slug = m.group(1)
        api_base = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
        return DiscoveredAts(
            ats_type="greenhouse",
            company_slug=slug,
            api_base=api_base,
            evidence_url=m.group(0),
        )

    m = _LEVER_RE.search(text)
    if m:
        slug = m.group(1)
        api_base = f"https://api.lever.co/v0/postings/{slug}"
        return DiscoveredAts(
            ats_type="lever",
            company_slug=slug,
            api_base=api_base,
            evidence_url=m.group(0),
        )

    m = _WORKABLE_RE.search(text)
    if m:
        slug = m.group(1)
        api_base = f"https://apply.workable.com/api/v1/accounts/{slug}/jobs"
        return DiscoveredAts(
            ats_type="workable",
            company_slug=slug,
            api_base=api_base,
            evidence_url=m.group(0),
        )

    m = _WORKDAY_RE.search(text)
    if m:
        tenant = m.group(1)
        site = m.group(3)
        parsed = urlparse(m.group(0))
        host = parsed.netloc
        api_base = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
        company_slug = f"{tenant}/{site}"
        return DiscoveredAts(
            ats_type="workday",
            company_slug=company_slug,
            api_base=api_base,
            evidence_url=m.group(0),
        )

    if "successfactors" in text.lower():
        m = _SFS_RE.search(text)
        if m:
            raw_url = m.group(0)
            parsed = urlparse(raw_url)
            qs = parse_qs(parsed.query)
            company = (qs.get("company") or [""])[0].strip()
            if company:
                base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                api_base = (
                    f"{base}?company={company}"
                    "&career_ns=job_listing_summary&resultType=XML"
                )
                return DiscoveredAts(
                    ats_type="successfactors",
                    company_slug=company,
                    api_base=api_base,
                    evidence_url=raw_url,
                )

    return None
