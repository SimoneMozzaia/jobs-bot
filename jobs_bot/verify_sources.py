from __future__ import annotations

import datetime as dt
import logging
import time

from sqlalchemy.orm import Session

from .ats_clients import (
    fetch_greenhouse_jobs_page,
    fetch_lever_postings,
    fetch_successfactors_postings,
    fetch_workable_jobs,
    fetch_workday_jobs_page,
)
from .models import Source

logger = logging.getLogger(__name__)


def _verify_one_source(src: Source, *, timeout_s: int) -> None:
    """Raise on failure, return None on success."""
    ats = (src.ats_type or "").strip().lower()

    if ats == "greenhouse":
        fetch_greenhouse_jobs_page(
            api_base=src.api_base,
            page=1,
            per_page=1,
            timeout_s=timeout_s,
        )
        return

    if ats == "lever":
        fetch_lever_postings(
            api_base=src.api_base,
            timeout_s=timeout_s,
        )
        return

    if ats == "workable":
        fetch_workable_jobs(
            api_base=src.api_base,
            timeout_s=timeout_s,
        )
        return

    if ats == "workday":
        fetch_workday_jobs_page(
            api_base=src.api_base,
            offset=0,
            limit=1,
            timeout_s=timeout_s,
        )
        return

    if ats == "successfactors":
        fetch_successfactors_postings(
            api_base=src.api_base,
            timeout_s=timeout_s,
        )
        return

    raise ValueError(f"Unsupported ats_type for verification: {src.ats_type!r}")


def verify_and_promote_sources(
    db: Session,
    sources: list[Source],
    *,
    timeout_s: int,
    delay_s: float,
) -> tuple[int, int]:
    """Verify inactive sources and promote successful ones.

    Returns: (verified_ok, verified_failed)
    """
    ok = 0
    failed = 0

    for src in sources:
        if delay_s:
            time.sleep(delay_s)
        try:
            _verify_one_source(src, timeout_s=timeout_s)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            src.last_error = f"verify_failed:{type(exc).__name__}"
            src.is_active = 0
            db.add(src)
            db.commit()
            logger.info(
                "Verification failed for source id=%s (%s)",
                src.id,
                src.last_error,
            )
            continue

        ok += 1
        now = dt.datetime.utcnow()
        src.is_active = 1
        src.verified_at = now
        src.last_ok_at = now
        src.last_error = None
        db.add(src)
        db.commit()
        logger.info("Verified & promoted source id=%s ats=%s", src.id, src.ats_type)

    return ok, failed
