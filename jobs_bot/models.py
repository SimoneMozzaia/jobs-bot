from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

_LONGTEXT = Text().with_variant(LONGTEXT, "mysql")
_BIGINT = BigInteger().with_variant(Integer, "sqlite")


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(_BIGINT, primary_key=True, autoincrement=True)
    ats_type: Mapped[str] = mapped_column(Enum("greenhouse", "lever"), nullable=False, index=True)
    company_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(255))
    api_base: Mapped[str] = mapped_column(String(512), nullable=False)

    is_active: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    discovered_via: Mapped[str] = mapped_column(
        Enum("manual", "brave", "serpapi"),
        nullable=False,
        server_default=text("'manual'"),
    )
    region_hint: Mapped[str | None] = mapped_column(String(64))
    verified_at: Mapped[dt.datetime | None] = mapped_column(DateTime)
    last_ok_at: Mapped[dt.datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        server_onupdate=text("CURRENT_TIMESTAMP"),
    )

    jobs: Mapped[list["Job"]] = relationship(back_populates="source")


class Job(Base):
    __tablename__ = "jobs"

    job_uid: Mapped[str] = mapped_column(String(40), primary_key=True)
    source_id: Mapped[int] = mapped_column(_BIGINT, ForeignKey("sources.id"), nullable=False)
    ats_job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    company: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    location_raw: Mapped[str | None] = mapped_column(String(512))
    workplace_raw: Mapped[str | None] = mapped_column(String(128))

    posted_at: Mapped[dt.datetime | None] = mapped_column(DateTime)
    first_seen: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False)
    last_seen: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False)

    last_checked: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    raw_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    raw_text: Mapped[str | None] = mapped_column(_LONGTEXT)
    salary_text: Mapped[str | None] = mapped_column(String(255))

    fit_score: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"), index=True)
    fit_class: Mapped[str] = mapped_column(
        Enum("Good", "Maybe", "No"), nullable=False, server_default=text("'No'")
    )
    penalty_flags: Mapped[dict | None] = mapped_column(JSON)

    notion_page_id: Mapped[str | None] = mapped_column(String(36))
    status: Mapped[str | None] = mapped_column(
        Enum("New", "Shortlist", "Applied", "Interview", "Offer", "Rejected", "Accepted"),
        nullable=True,
    )

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        server_onupdate=text("CURRENT_TIMESTAMP"),
    )

    notion_last_sync: Mapped[dt.datetime | None] = mapped_column(DateTime)
    notion_last_error: Mapped[str | None] = mapped_column(Text)

    source: Mapped["Source"] = relationship(back_populates="jobs")
    enrichment: Mapped["JobEnrichment"] = relationship(back_populates="job", uselist=False)
    profiles: Mapped[list["JobProfile"]] = relationship(back_populates="job")


class JobEnrichment(Base):
    __tablename__ = "job_enrichment"

    job_uid: Mapped[str] = mapped_column(String(40), ForeignKey("jobs.job_uid"), primary_key=True)

    summary: Mapped[str | None] = mapped_column(Text)
    skills_json: Mapped[dict | None] = mapped_column(JSON)
    pros: Mapped[str | None] = mapped_column(Text)
    cons: Mapped[str | None] = mapped_column(Text)
    outreach_target: Mapped[str | None] = mapped_column(String(512))

    salary: Mapped[str | None] = mapped_column(String(255))
    llm_model: Mapped[str | None] = mapped_column(String(128))
    llm_tokens: Mapped[int | None] = mapped_column(Integer)
    enriched_at: Mapped[dt.datetime | None] = mapped_column(DateTime)

    job: Mapped["Job"] = relationship(back_populates="enrichment")


class Profile(Base):
    __tablename__ = "profiles"

    profile_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    cv_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    cv_sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    profile_json: Mapped[dict | None] = mapped_column(JSON)
    profile_text: Mapped[str | None] = mapped_column(Text)
    analyzed_at: Mapped[dt.datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        nullable=False,
    )


class JobProfile(Base):
    __tablename__ = "job_profile"

    job_uid: Mapped[str] = mapped_column(String(40), ForeignKey("jobs.job_uid"), primary_key=True)
    profile_id: Mapped[str] = mapped_column(String(64), ForeignKey("profiles.profile_id"), primary_key=True)

    fit_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    fit_class: Mapped[str] = mapped_column(
        Enum("Good", "Maybe", "No", name="fit_class_profile"),
        nullable=False,
        default="No",
    )
    penalty_flags: Mapped[dict | None] = mapped_column(JSON)

    # Deterministic invalidation keys
    fit_job_last_checked: Mapped[dt.datetime | None] = mapped_column(DateTime)
    fit_profile_cv_sha256: Mapped[str | None] = mapped_column(String(64))
    fit_computed_at: Mapped[dt.datetime | None] = mapped_column(DateTime)

    notion_page_id: Mapped[str | None] = mapped_column(String(36))
    notion_last_sync: Mapped[dt.datetime | None] = mapped_column(DateTime)
    notion_last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        nullable=False,
    )

    job: Mapped["Job"] = relationship(back_populates="profiles")
