from __future__ import annotations

import re
import time
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests

from .html_extract import extract_anchors


@dataclass(frozen=True)
class CareersDiscoveryResult:
    company_homepage: str
    careers_url: str | None
    reason: str


_CAREER_WORDS_RE = re.compile(
    r"(?i)\b("
    r"career|careers|jobs|job|join\s*us|work\s*with\s*us|"
    r"lavora\s*con\s*noi|carriere|karriere|emplois|carrières"
    r")\b"
)


def _same_domain(a: str, b: str) -> bool:
    pa = urlparse(a)
    pb = urlparse(b)
    if not pa.netloc or not pb.netloc:
        return False
    return pa.netloc.lower() == pb.netloc.lower()


def _score_anchor(href: str, text: str) -> int:
    h = (href or "").lower()
    t = (text or "").lower()
    score = 0

    if "careers" in h or "career" in h:
        score += 100
    if "jobs" in h or "/job" in h:
        score += 90
    if "join" in h or "work-with" in h:
        score += 50
    if _CAREER_WORDS_RE.search(t):
        score += 60

    if h.startswith(("mailto:", "tel:")):
        score -= 500
    if any(x in h for x in ("facebook.com", "linkedin.com", "instagram.com")):
        score -= 80

    return score


def discover_careers_url(
    company_homepage: str,
    *,
    session: requests.Session,
    timeout_s: int = 20,
    delay_s: float = 0.0,
    user_agent: str | None = None,
) -> CareersDiscoveryResult:
    """Find a company's careers page URL starting from the homepage (polite)."""

    homepage = (company_homepage or "").strip()
    if not homepage:
        return CareersDiscoveryResult(
            company_homepage="",
            careers_url=None,
            reason="empty_homepage",
        )

    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent

    if delay_s:
        time.sleep(delay_s)

    try:
        resp = session.get(homepage, headers=headers, timeout=timeout_s)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return CareersDiscoveryResult(
            company_homepage=homepage,
            careers_url=None,
            reason=f"homepage_fetch_failed:{type(exc).__name__}",
        )

    html = resp.text or ""
    anchors = extract_anchors(html)
    if not anchors:
        return CareersDiscoveryResult(
            company_homepage=resp.url,
            careers_url=None,
            reason="no_anchors",
        )

    scored: list[tuple[int, str]] = []
    for a in anchors:
        href = (a.href or "").strip()
        if not href:
            continue

        abs_url = urljoin(resp.url, href)
        score = _score_anchor(abs_url, a.text or "")

        if not _same_domain(resp.url, abs_url):
            h = abs_url.lower()
            if any(
                dom in h
                for dom in (
                    "greenhouse.io",
                    "lever.co",
                    "workable.com",
                    "myworkdayjobs.com",
                    "successfactors",
                )
            ):
                score += 15
            else:
                score -= 20

        if score > 0:
            scored.append((score, abs_url))

    if scored:
        scored.sort(reverse=True, key=lambda x: x[0])
        return CareersDiscoveryResult(
            company_homepage=resp.url,
            careers_url=scored[0][1],
            reason="link_found",
        )

    parsed = urlparse(resp.url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in ("/careers", "/careers/", "/jobs", "/jobs/"):
        candidate = f"{base}{path}"
        if delay_s:
            time.sleep(delay_s)
        try:
            r2 = session.get(
                candidate,
                headers=headers,
                timeout=timeout_s,
                allow_redirects=True,
            )
            if 200 <= r2.status_code < 400:
                return CareersDiscoveryResult(
                    company_homepage=resp.url,
                    careers_url=r2.url,
                    reason="fallback_path",
                )
        except requests.RequestException:
            continue

    return CareersDiscoveryResult(
        company_homepage=resp.url,
        careers_url=None,
        reason="no_candidate_links",
    )
