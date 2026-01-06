from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from .config import Settings


def _ensure_pymysql_crypto_available(mysql_url: str) -> None:
    """Fail fast with a clear message for common MySQL 8 auth defaults.

    MySQL 8 defaults to caching_sha2_password for users; PyMySQL requires the
    'cryptography' package to perform RSA-based auth for sha256/caching_sha2.
    """

    if not (mysql_url or "").startswith("mysql+pymysql://"):
        return

    try:
        import cryptography  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency 'cryptography'. It is required by PyMySQL when "
            "connecting to MySQL 8 users using sha256_password/caching_sha2_password. "
            "Fix: pip install -r requirements.txt (or 'pip install cryptography'), "
            "or configure the MySQL user to use mysql_native_password."
        ) from exc


def make_engine(settings: Settings) -> Engine:
    _ensure_pymysql_crypto_available(settings.mysql_url)
    return create_engine(
        settings.mysql_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True,
    )


def make_session_factory(settings: Settings) -> sessionmaker:
    engine = make_engine(settings)
    return sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )
