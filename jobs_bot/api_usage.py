from __future__ import annotations

import datetime as dt

from sqlalchemy import text
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Session


def utcnow_naive() -> dt.datetime:
    """UTC naive timestamp (tzinfo=None) to match DB columns stored as naive UTC."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _utc_day() -> dt.date:
    return dt.datetime.now(dt.UTC).date()


def _dialect(session: Session) -> Dialect:
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine/connection.")
    return bind.dialect


def _is_sqlite(session: Session) -> bool:
    return _dialect(session).name == "sqlite"


def _sqlite_table_exists(session: Session, table: str) -> bool:
    """
    SQLite-only helper. Avoids raising OperationalError in tests when fixtures
    do not create the counter tables.
    """
    res = session.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:t LIMIT 1"),
        {"t": table},
    ).scalar_one_or_none()
    return res is not None


def _seed_counter_row_mysql(
    session: Session,
    *,
    table: str,
    insert_cols: tuple[str, ...],
    insert_params: dict,
    noop_col: str,
) -> None:
    cols_sql = ", ".join(insert_cols)
    vals_sql = ", ".join(f":{c}" for c in insert_cols)
    session.execute(
        text(
            f"""
            INSERT INTO {table} ({cols_sql})
            VALUES ({vals_sql})
            ON DUPLICATE KEY UPDATE {noop_col} = {noop_col}
            """
        ),
        insert_params,
    )


def _seed_counter_row_sqlite(
    session: Session,
    *,
    table: str,
    insert_cols: tuple[str, ...],
    insert_params: dict,
    where_keys: tuple[str, ...],
) -> None:
    """
    SQLite-safe seed that does NOT require UNIQUE constraints.
    Inserts only if no row exists (best-effort; fine for tests).
    """
    cols_sql = ", ".join(insert_cols)
    vals_sql = ", ".join(f":{c}" for c in insert_cols)
    where_sql = " AND ".join(f"{k} = :{k}" for k in where_keys)

    session.execute(
        text(
            f"""
            INSERT INTO {table} ({cols_sql})
            SELECT {vals_sql}
            WHERE NOT EXISTS (
                SELECT 1 FROM {table} WHERE {where_sql}
            )
            """
        ),
        insert_params,
    )


def can_consume_call(session: Session, ats_type: str, max_per_day: int) -> bool:
    """
    Provider-level daily API call cap.

    True  -> consumed 1 call (counter incremented)
    False -> cap reached (no increment)

    Notes:
    - On SQLite (tests), if the counter table is missing, we treat it as unlimited
      (return True) rather than failing ingestion.
    """
    if max_per_day <= 0:
        return True

    day = _utc_day()

    if _is_sqlite(session) and not _sqlite_table_exists(session, "api_daily_usage"):
        return True

    if _is_sqlite(session):
        _seed_counter_row_sqlite(
            session,
            table="api_daily_usage",
            insert_cols=("day", "ats_type", "calls"),
            insert_params={"day": day, "ats_type": ats_type, "calls": 0},
            where_keys=("day", "ats_type"),
        )
    else:
        _seed_counter_row_mysql(
            session,
            table="api_daily_usage",
            insert_cols=("day", "ats_type", "calls"),
            insert_params={"day": day, "ats_type": ats_type, "calls": 0},
            noop_col="calls",
        )

    res = session.execute(
        text(
            """
            UPDATE api_daily_usage
            SET calls = calls + 1
            WHERE day = :day
              AND ats_type = :ats_type
              AND calls < :max_per_day
            """
        ),
        {"day": day, "ats_type": ats_type, "max_per_day": max_per_day},
    )

    # On SQLite, in edge cases rowcount may be >1 if the fixture created duplicates.
    return (res.rowcount or 0) >= 1


def can_create_new_job(session: Session, max_new_per_day: int) -> bool:
    """
    Daily cap on NEW job inserts (not updates).

    True  -> reserved 1 "new job slot" (counter incremented)
    False -> cap reached (no increment)

    Notes:
    - On SQLite (tests), if the counter table is missing, we treat it as unlimited.
    """
    if max_new_per_day <= 0:
        return True

    day = _utc_day()

    if _is_sqlite(session) and not _sqlite_table_exists(session, "job_daily_new"):
        return True

    if _is_sqlite(session):
        _seed_counter_row_sqlite(
            session,
            table="job_daily_new",
            insert_cols=("day", "created"),
            insert_params={"day": day, "created": 0},
            where_keys=("day",),
        )
    else:
        _seed_counter_row_mysql(
            session,
            table="job_daily_new",
            insert_cols=("day", "created"),
            insert_params={"day": day, "created": 0},
            noop_col="created",
        )

    res = session.execute(
        text(
            """
            UPDATE job_daily_new
            SET created = created + 1
            WHERE day = :day
              AND created < :max_new_per_day
            """
        ),
        {"day": day, "max_new_per_day": max_new_per_day},
    )
    return (res.rowcount or 0) >= 1
