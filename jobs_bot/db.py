from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import Settings


def make_engine(settings: Settings):
    return create_engine(
        settings.mysql_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True,
    )


def make_session_factory(settings: Settings):
    engine = make_engine(settings)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
