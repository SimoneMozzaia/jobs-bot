from __future__ import annotations

import datetime as dt

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

_API_DAILY_USAGE_TABLE = "api_daily_usage"
_DAILY_NEW_JOBS_TABLE = "daily_new_jobs"


def utcnow_naive() -> dt.datetime:
    """Return a timezone-naive UTC timestamp."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def today_utc_date() -> str:
    """Return today's UTC date as YYYY-MM-DD."""
    return dt.datetime.now(dt.UTC).date().isoformat()


def _dialect(session: Session) -> str:
    return session.get_bind().dialect.name


def _ensure_api_usage_tables(session: Session) -> None:
    """Create the internal usage tables if they don't exist.

    These tables are created with raw SQL (not via ORM models) because they are
    operational counters rather than domain entities.
    """
    dialect = _dialect(session)

    if dialect == "sqlite":
        session.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_API_DAILY_USAGE_TABLE} (
                    day TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    calls INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (day, provider)
                )
                """
            )
        )
        session.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_DAILY_NEW_JOBS_TABLE} (
                    day TEXT NOT NULL PRIMARY KEY,
                    new_jobs INTEGER NOT NULL DEFAULT 0
                )
                """
            )
        )
    else:
        # MySQL / MariaDB
        session.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_API_DAILY_USAGE_TABLE} (
                    day DATE NOT NULL,
                    provider VARCHAR(32) NOT NULL,
                    calls INT NOT NULL DEFAULT 0,
                    PRIMARY KEY (day, provider)
                ) ENGINE=InnoDB
                """
            )
        )
        session.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_DAILY_NEW_JOBS_TABLE} (
                    day DATE NOT NULL PRIMARY KEY,
                    new_jobs INT NOT NULL DEFAULT 0
                ) ENGINE=InnoDB
                """
            )
        )

    session.flush()


def _is_missing_column_error(exc: OperationalError, column: str) -> bool:
    msg = str(exc).lower()
    if f"no column named {column}" in msg:
        return True
    if f"unknown column '{column}'" in msg:
        return True
    if f"unknown column `{column}`" in msg:
        return True
    return False


def _ensure_usage_row(session: Session, *, day: str, column: str, provider: str) -> None:
    dialect = _dialect(session)

    if dialect == "sqlite":
        session.execute(
            text(
                f"""
                INSERT INTO {_API_DAILY_USAGE_TABLE} (day, {column}, calls)
                SELECT :day, :provider, 0
                WHERE NOT EXISTS (
                    SELECT 1 FROM {_API_DAILY_USAGE_TABLE}
                    WHERE day = :day AND {column} = :provider
                )
                """
            ),
            {"day": day, "provider": provider},
        )
    else:
        session.execute(
            text(
                f"""
                INSERT IGNORE INTO {_API_DAILY_USAGE_TABLE} (day, {column}, calls)
                VALUES (:day, :provider, 0)
                """
            ),
            {"day": day, "provider": provider},
        )


def _consume_call_with_column(
    session: Session, *, day: str, column: str, provider: str, max_per_day: int
) -> bool:
    _ensure_usage_row(session, day=day, column=column, provider=provider)

    if max_per_day <= 0:
        session.execute(
            text(
                f"""
                UPDATE {_API_DAILY_USAGE_TABLE}
                SET calls = calls + 1
                WHERE day = :day AND {column} = :provider
                """
            ),
            {"day": day, "provider": provider},
        )
        return True

    result = session.execute(
        text(
            f"""
            UPDATE {_API_DAILY_USAGE_TABLE}
            SET calls = calls + 1
            WHERE day = :day AND {column} = :provider AND calls < :max_per_day
            """
        ),
        {"day": day, "provider": provider, "max_per_day": max_per_day},
    )
    return (result.rowcount or 0) == 1


def can_consume_call(session: Session, provider: str, *, max_per_day: int) -> bool:
    """Consume one API call from the daily provider bucket.

    Returns True if the call can be consumed (i.e., below the daily cap),
    otherwise False.
    """
    if not provider:
        raise ValueError("provider must be a non-empty string")

    _ensure_api_usage_tables(session)
    day = today_utc_date()

    try:
        return _consume_call_with_column(
            session,
            day=day,
            column="provider",
            provider=provider,
            max_per_day=max_per_day,
        )
    except OperationalError as e:
        # Backwards compatibility: older deployments used `ats_type` as the column
        # name for the provider key.
        if _is_missing_column_error(e, "provider"):
            return _consume_call_with_column(
                session,
                day=day,
                column="ats_type",
                provider=provider,
                max_per_day=max_per_day,
            )
        raise


def can_create_new_job(session: Session, *, max_new_per_day: int) -> bool:
    """Consume one unit from the daily NEW job creation counter."""
    _ensure_api_usage_tables(session)
    day = today_utc_date()
    dialect = _dialect(session)

    if dialect == "sqlite":
        session.execute(
            text(
                f"""
                INSERT INTO {_DAILY_NEW_JOBS_TABLE} (day, new_jobs)
                SELECT :day, 0
                WHERE NOT EXISTS (
                    SELECT 1 FROM {_DAILY_NEW_JOBS_TABLE} WHERE day = :day
                )
                """
            ),
            {"day": day},
        )
    else:
        session.execute(
            text(
                f"""
                INSERT IGNORE INTO {_DAILY_NEW_JOBS_TABLE} (day, new_jobs)
                VALUES (:day, 0)
                """
            ),
            {"day": day},
        )

    if max_new_per_day <= 0:
        session.execute(
            text(
                f"""
                UPDATE {_DAILY_NEW_JOBS_TABLE}
                SET new_jobs = new_jobs + 1
                WHERE day = :day
                """
            ),
            {"day": day},
        )
        return True

    result = session.execute(
        text(
            f"""
            UPDATE {_DAILY_NEW_JOBS_TABLE}
            SET new_jobs = new_jobs + 1
            WHERE day = :day AND new_jobs < :max_new
            """
        ),
        {"day": day, "max_new": max_new_per_day},
    )
    return (result.rowcount or 0) == 1
