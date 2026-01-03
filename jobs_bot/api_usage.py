from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session
import datetime as dt


def utcnow_naive() -> dt.datetime:
    # Python 3.13: evitare utcnow() deprecato
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _utc_day() -> dt.date:
    return dt.datetime.now(dt.UTC).date()


def can_consume_call(session: Session, ats_type: str, max_per_day: int) -> bool:
    """
    Provider-level daily cap, strict + concurrency-safe.
    True = consumata 1 call; False = cap raggiunto (nessun incremento).
    """

    if max_per_day <= 0:
        return True  # 0/negativo = unlimited

    day = _utc_day()

    # Ensure row exists
    session.execute(
        text(
            """
            INSERT INTO api_daily_usage(day, ats_type, calls)
            VALUES (:day, :ats_type, 0)
            ON DUPLICATE KEY UPDATE calls = calls
            """
        ),
        {"day": day, "ats_type": ats_type},
    )

    # Lock row (atomic check+increment)
    calls = session.execute(
        text(
            """
            SELECT calls
            FROM api_daily_usage
            WHERE day = :day AND ats_type = :ats_type
            FOR UPDATE
            """
        ),
        {"day": day, "ats_type": ats_type},
    ).scalar_one()

    if calls >= max_per_day:
        return False

    session.execute(
        text(
            """
            UPDATE api_daily_usage
            SET calls = calls + 1
            WHERE day = :day AND ats_type = :ats_type
            """
        ),
        {"day": day, "ats_type": ats_type},
    )
    return True

def can_create_new_job(session: Session, max_new_per_day: int) -> bool:
    if max_new_per_day <= 0:
        return True  # unlimited

    day = dt.datetime.now(dt.UTC).date()

    session.execute(
        text("""
            INSERT INTO job_daily_new(day, created)
            VALUES (:day, 0)
            ON DUPLICATE KEY UPDATE created = created
        """),
        {"day": day},
    )

    created = session.execute(
        text("""
            SELECT created
            FROM job_daily_new
            WHERE day = :day
            FOR UPDATE
        """),
        {"day": day},
    ).scalar_one()

    if created >= max_new_per_day:
        return False

    session.execute(
        text("""
            UPDATE job_daily_new
            SET created = created + 1
            WHERE day = :day
        """),
        {"day": day},
    )
    return True