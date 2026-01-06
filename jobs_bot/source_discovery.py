from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .ats_discovery import DiscoveredAts, detect_ats_from_url_or_html
from .careers_discovery import discover_careers_url
from .companies_marketcap import fetch_top_companies
from .models import Source
from .wikidata import resolve_official_website

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveryCounts:
    companies_seen: int = 0
    websites_resolved: int = 0
    careers_found: int = 0
    ats_detected: int = 0
    sources_upserted: int = 0


def parse_regions(regions_csv: str) -> list[str]:
    regions = []
    for raw in (regions_csv or "").split(","):
        r = raw.strip().upper()
        if not r:
            continue
        if r in ("CA", "CAN"):
            r = "CANADA"
        if r in ("AE",):
            r = "UAE"
        regions.append(r)

    dedup: list[str] = []
    for r in regions:
        if r not in dedup:
            dedup.append(r)
    return dedup


def _ensure_http(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    parsed = urlparse(u)
    if parsed.scheme:
        return u
    return "https://" + u


def _upsert_source(
    db: Session,
    *,
    region: str,
    company_name: str,
    discovered: DiscoveredAts,
    discovered_via_prefix: str = "cmc",
) -> tuple[Source, bool]:
    """Upsert a Source by (ats_type, company_slug)."""

    existing = db.execute(
        select(Source).where(
            Source.ats_type == discovered.ats_type,
            Source.company_slug == discovered.company_slug,
        )
    ).scalar_one_or_none()

    discovered_via = f"{discovered_via_prefix}:{region}"

    if existing is None:
        src = Source(
            ats_type=discovered.ats_type,
            company_slug=discovered.company_slug,
            api_base=discovered.api_base,
            company_name=company_name,
            discovered_via=discovered_via,
            is_active=0,
            last_ok_at=None,
            last_error=None,
            verified_at=None,
        )
        db.add(src)
        db.flush()
        return src, True

    updated = False
    if (existing.api_base or "") != discovered.api_base:
        existing.api_base = discovered.api_base
        updated = True
    if (existing.company_name or "") != company_name:
        existing.company_name = company_name
        updated = True
    if (existing.discovered_via or "") != discovered_via:
        existing.discovered_via = discovered_via
        updated = True

    if updated and existing.verified_at is not None:
        existing.is_active = 0
        existing.verified_at = None
        existing.last_ok_at = None
        existing.last_error = "invalidated_by_discovery_update"

    return existing, False


def discover_sources_from_companiesmarketcap(
    db: Session,
    *,
    region: str,
    max_companies: int,
    max_sources_to_upsert: int,
    request_timeout_s: int,
    delay_s: float,
    http_user_agent: str,
    wikidata_user_agent: str,
    wikidata_name_fallback_enable: int = 0,
) -> DiscoveryCounts:
    """Discover Sources for a given region.

    Flow:
      CompaniesMarketCap -> Wikidata website -> homepage -> careers URL
      -> ATS detection -> sources upsert (inactive).
    """

    counts = DiscoveryCounts()
    http = requests.Session()
    wik = requests.Session()

    companies = fetch_top_companies(
        region,
        max_companies=max_companies,
        session=http,
        timeout_s=request_timeout_s,
        user_agent=http_user_agent,
    )

    for comp in companies:
        if counts.sources_upserted >= max_sources_to_upsert:
            break

        counts.companies_seen += 1

        try:
            ent = resolve_official_website(
                company_name=comp.name,
                ticker=comp.ticker,
                session=wik,
                timeout_s=request_timeout_s,
                user_agent=wikidata_user_agent,
                allow_name_fallback=bool(wikidata_name_fallback_enable),
            )
        except requests.RequestException as exc:
            logger.debug(
                "Wikidata lookup failed for %s: %s",
                comp.name,
                type(exc).__name__,
            )
            continue

        website = (ent.official_website if ent else None) or ""
        website = _ensure_http(website)
        if not website:
            continue

        counts.websites_resolved += 1

        careers = discover_careers_url(
            website,
            session=http,
            timeout_s=request_timeout_s,
            delay_s=delay_s,
            user_agent=http_user_agent,
        )
        if not careers.careers_url:
            continue

        counts.careers_found += 1

        if delay_s:
            time.sleep(delay_s)
        try:
            resp = http.get(
                careers.careers_url,
                headers={"User-Agent": http_user_agent},
                timeout=request_timeout_s,
                allow_redirects=True,
            )
            resp.raise_for_status()
        except requests.RequestException:
            continue

        discovered = detect_ats_from_url_or_html(
            page_url=resp.url,
            html=resp.text or "",
        )
        if not discovered:
            continue

        counts.ats_detected += 1

        _src, _created = _upsert_source(
            db,
            region=region,
            company_name=comp.name,
            discovered=discovered,
        )

        counts.sources_upserted += 1
        db.commit()

    return counts


def iter_inactive_sources_for_verification(
    db: Session,
    *,
    limit: int,
    discovered_via_prefix: str = "cmc",
) -> Iterable[Source]:
    stmt = (
        select(Source)
        .where(Source.is_active == 0)
        .where(Source.discovered_via.like(f"{discovered_via_prefix}:%"))
        .order_by(Source.id.asc())
        .limit(int(limit))
    )
    return db.execute(stmt).scalars().all()
