"""Helpers to upsert discovered sources in an idempotent way."""

from __future__ import annotations

import datetime as dt

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Source


def upsert_source(
    session: Session,
    *,
    ats_type: str,
    company_slug: str,
    company_name: str,
    api_base: str,
    discovered_via: str,
    is_active: int,
    now: dt.datetime | None = None,
) -> Source:
    """Idempotent upsert without relying on DB unique constraints."""
    now = now or dt.datetime.utcnow()

    existing = (
        session.execute(
            select(Source).where(
                Source.ats_type == ats_type,
                Source.company_slug == company_slug,
                Source.api_base == api_base,
            )
        )
        .scalars()
        .first()
    )

    if existing:
        existing.company_name = company_name or existing.company_name
        existing.discovered_via = discovered_via or existing.discovered_via
        existing.is_active = int(is_active)
        session.flush()
        return existing

    src = Source(
        ats_type=ats_type,
        company_slug=company_slug,
        company_name=company_name,
        api_base=api_base,
        is_active=int(is_active),
        discovered_via=discovered_via,
        last_ok_at=None,
        last_error=None,
        verified_at=None,
        created_at=now,
        updated_at=now,
    )
    session.add(src)
    session.flush()
    return src
