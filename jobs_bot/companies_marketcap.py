from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from typing import Iterator

import requests


@dataclass(frozen=True)
class CmcCompany:
    """Minimal company record sourced from CompaniesMarketCap."""

    name: str
    ticker: str | None
    country: str | None
    rank: int | None
    detail_url: str | None = None


_REGION_PATHS: dict[str, str] = {
    "US": "usa/largest-companies-in-the-usa-by-market-cap/",
    "CANADA": "canada/largest-companies-in-canada-by-market-cap/",
    "EU": "european-union/largest-companies-in-the-eu-by-market-cap/",
    "CH": "switzerland/largest-companies-in-switzerland-by-market-cap/",
    "UAE": "united-arab-emirates/largest-companies-in-the-uae-by-market-cap/",
}


def normalize_region(region: str) -> str:
    r = (region or "").strip().upper()
    if r in ("CA", "CAN"):
        return "CANADA"
    if r in ("AE", "UNITEDARABEMIRATES"):
        return "UAE"
    return r


def cmc_region_csv_url(region: str, *, currency: str | None = None) -> str:
    """Build CompaniesMarketCap CSV download URL via ?download=csv."""
    reg = normalize_region(region)
    if reg not in _REGION_PATHS:
        raise ValueError(f"Unsupported region: {region!r}")

    path = _REGION_PATHS[reg].lstrip("/")
    if currency:
        return (
            f"https://companiesmarketcap.com/{currency.strip().lower()}/{path}"
            "?download=csv"
        )
    return f"https://companiesmarketcap.com/{path}?download=csv"


def cmc_region_html_url(region: str, *, page: int = 1) -> str:
    reg = normalize_region(region)
    if reg not in _REGION_PATHS:
        raise ValueError(f"Unsupported region: {region!r}")

    base = f"https://companiesmarketcap.com/{_REGION_PATHS[reg].lstrip('/')}"
    if page and page > 1:
        return f"{base}?page={int(page)}"
    return base


_CLEAN_WS_RE = re.compile(r"\s+")
_DETAIL_HREF_RE = re.compile(r'href="(/[^\"]+/marketcap/)"')


def _clean(s: str | None) -> str:
    return _CLEAN_WS_RE.sub(" ", (s or "").strip())


def _maybe_int(s: str | None) -> int | None:
    if s is None:
        return None
    t = s.strip()
    if not t:
        return None
    if not t.isdigit():
        return None
    return int(t)


def parse_cmc_csv(text: str) -> list[CmcCompany]:
    """Parse CompaniesMarketCap CSV exports (headerless or with header)."""
    if not text or not text.strip():
        return []

    raw = text.lstrip("\ufeff")
    reader = csv.reader(io.StringIO(raw))
    rows = list(reader)
    if not rows:
        return []

    start_idx = 0
    if rows and rows[0] and _maybe_int(rows[0][0]) is None:
        start_idx = 1

    out: list[CmcCompany] = []
    for r in rows[start_idx:]:
        if not r:
            continue
        rank = _maybe_int(r[0]) if len(r) >= 1 else None
        name = _clean(r[1]) if len(r) >= 2 else ""
        ticker = _clean(r[2]) if len(r) >= 3 else ""
        country = _clean(r[-1]) if len(r) >= 1 else ""

        if not name:
            continue

        out.append(
            CmcCompany(
                name=name,
                ticker=ticker or None,
                country=country or None,
                rank=rank,
                detail_url=None,
            )
        )

    return out


def parse_cmc_company_links(html: str) -> Iterator[str]:
    """Best-effort extraction of company detail links from HTML (fallback)."""
    for m in _DETAIL_HREF_RE.finditer(html or ""):
        href = (m.group(1) or "").strip()
        if href:
            yield f"https://companiesmarketcap.com{href}"


def fetch_top_companies(
    region: str,
    *,
    max_companies: int,
    session: requests.Session,
    timeout_s: int = 20,
    user_agent: str | None = None,
) -> list[CmcCompany]:
    """Fetch up to max_companies for a region.

    Strategy:
      1) Try CSV export (preferred).
      2) Fallback to HTML pagination parsing if CSV is unavailable.
    """
    reg = normalize_region(region)
    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent

    for currency in (None, "usd"):
        url = cmc_region_csv_url(reg, currency=currency)
        try:
            resp = session.get(url, headers=headers, timeout=timeout_s)
            if resp.status_code >= 400:
                continue
            companies = parse_cmc_csv(resp.text)
            if companies:
                return companies[:max_companies]
        except requests.RequestException:
            continue

    out: list[CmcCompany] = []
    page = 1
    while len(out) < max_companies:
        url = cmc_region_html_url(reg, page=page)
        resp = session.get(url, headers=headers, timeout=timeout_s)
        resp.raise_for_status()
        html = resp.text or ""

        links = list(parse_cmc_company_links(html))
        if not links:
            break

        for link in links:
            out.append(
                CmcCompany(
                    name=link.rsplit("/", 3)[-3],
                    ticker=None,
                    country=None,
                    rank=None,
                    detail_url=link,
                )
            )
            if len(out) >= max_companies:
                break

        page += 1

    return out
