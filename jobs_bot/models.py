from __future__ import annotations

import datetime as dt

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, JSON, String, Text, text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ats_type: Mapped[str] = mapped_column(String(32), nullable=False)
    company_slug: Mapped[str] = mapped_column(String(128), nullable=False)
    company_name: Mapped[str] = mapped_column(String(256), nullable=False)
    api_base: Mapped[str] = mapped_column(String(512), nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    discovered_via: Mapped[str] = mapped_column(String(64), nullable=False, default="manual")

    last_ok_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    verified_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="source")



class Job(Base):
    __tablename__ = "jobs"

    job_uid: Mapped[str] = mapped_column(String(40), primary_key=True)  # SHA1 hex
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    ats_job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    company: Mapped[str] = mapped_column(String(256), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    location_raw: Mapped[str | None] = mapped_column(String(512), nullable=True)
    workplace_raw: Mapped[str | None] = mapped_column(String(64), nullable=True)
    salary_text: Mapped[str | None] = mapped_column(String(512), nullable=True)

    first_seen: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False)
    last_seen: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False)
    last_checked: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False)

    # JSON payloads are dynamic: keep typing robust and SQLAlchemy-friendly.
    raw_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Backward-compat (do not use for operational logic)
    fit_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fit_class: Mapped[str] = mapped_column(
        Enum("Good", "Maybe", "No", name="fit_class_enum"),
        nullable=False,
        default="No",
    )

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    source: Mapped["Source"] = relationship("Source", back_populates="jobs")
    enrichment: Mapped["JobEnrichment | None"] = relationship(
        "JobEnrichment", back_populates="job", uselist=False
    )
    profiles: Mapped[list["JobProfile"]] = relationship("JobProfile", back_populates="job")



class JobEnrichment(Base):
    __tablename__ = "job_enrichment"

    job_uid: Mapped[str] = mapped_column(ForeignKey("jobs.job_uid"), primary_key=True)

    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    skills_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    pros: Mapped[str | None] = mapped_column(Text, nullable=True)
    cons: Mapped[str | None] = mapped_column(Text, nullable=True)
    salary: Mapped[str | None] = mapped_column(String(512), nullable=True)

    outreach_target: Mapped[str | None] = mapped_column(String(256), nullable=True)

    enriched_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    llm_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    llm_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    job: Mapped["Job"] = relationship("Job", back_populates="enrichment")


class Profile(Base):
    __tablename__ = "profiles"

    profile_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    cv_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    cv_sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    profile_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)

    analyzed_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )


class JobProfile(Base):
    __tablename__ = "job_profile"

    job_uid: Mapped[str] = mapped_column(ForeignKey("jobs.job_uid"), primary_key=True)
    profile_id: Mapped[str] = mapped_column(ForeignKey("profiles.profile_id"), primary_key=True)

    fit_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fit_class: Mapped[str] = mapped_column(
        Enum("Good", "Maybe", "No", name="fit_class_enum_profile"),
        nullable=False,
        default="No",
    )
    penalty_flags: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)

    fit_job_last_checked: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    fit_profile_cv_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fit_computed_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    notion_page_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    notion_last_sync: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    notion_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    job: Mapped["Job"] = relationship("Job", back_populates="profiles")



class ApiDailyUsage(Base):
    """Daily API call counter per provider.

    This table is used by :func:`jobs_bot.api_usage.can_consume_call`.
    It intentionally uses a simple (day, ats_type) composite primary key
    so that the "ensure row exists + guarded UPDATE" logic is portable across
    SQLite (tests) and MySQL (runtime).
    """

    __tablename__ = "api_daily_usage"

    day: Mapped[str] = mapped_column(String(10), primary_key=True)  # YYYY-MM-DD
    ats_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    calls: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))


class JobDailyNew(Base):
    """Daily counter for NEW job insertions.

    Used by :func:`jobs_bot.api_usage.can_create_new_job`.
    """

    __tablename__ = "job_daily_new"

    day: Mapped[str] = mapped_column(String(10), primary_key=True)  # YYYY-MM-DD
    created: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
