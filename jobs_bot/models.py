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
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ats_type: Mapped[str] = mapped_column(Enum("greenhouse", "lever"), nullable=False)
    company_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
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

    job_uid: Mapped[str] = mapped_column(String(40), primary_key=True)  # sha1 hex
    source_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("sources.id"), nullable=False)
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

    fit_score: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
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

    raw_text: Mapped[str | None] = mapped_column(Text)
    salary_text: Mapped[str | None] = mapped_column(String(255))


class JobEnrichment(Base):
    __tablename__ = "job_enrichment"

    job_uid: Mapped[str] = mapped_column(String(40), ForeignKey("jobs.job_uid"), primary_key=True)

    summary: Mapped[str | None] = mapped_column(Text)
    skills_json: Mapped[dict | None] = mapped_column(JSON)
    pros: Mapped[str | None] = mapped_column(Text)
    cons: Mapped[str | None] = mapped_column(Text)
    outreach_target: Mapped[str | None] = mapped_column(String(512))

    # columns that exist in DB
    salary: Mapped[str | None] = mapped_column(String(255))
    llm_model: Mapped[str | None] = mapped_column(String(128))
    llm_tokens: Mapped[int | None] = mapped_column(Integer)
    enriched_at: Mapped[dt.datetime | None] = mapped_column(DateTime)

    job: Mapped["Job"] = relationship(back_populates="enrichment")

