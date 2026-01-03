from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    BigInteger, String, Text, DateTime, Enum, JSON, ForeignKey, Integer
)


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ats_type: Mapped[str] = mapped_column(Enum("greenhouse", "lever"), nullable=False)
    company_slug: Mapped[str] = mapped_column(String(128), nullable=False)
    api_base: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    jobs: Mapped[list["Job"]] = relationship(back_populates="source")


class Job(Base):
    __tablename__ = "jobs"

    job_uid: Mapped[str] = mapped_column(String(40), primary_key=True)  # sha1 hex
    source_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("sources.id"), nullable=False)
    ats_job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    company: Mapped[str] = mapped_column(String(256), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    location_raw: Mapped[str | None] = mapped_column(String(512))
    workplace_raw: Mapped[str | None] = mapped_column(String(128))

    posted_at: Mapped[str | None] = mapped_column(DateTime)
    first_seen: Mapped[str] = mapped_column(DateTime, nullable=False)
    last_seen: Mapped[str] = mapped_column(DateTime, nullable=False)
    last_checked: Mapped[str] = mapped_column(DateTime, nullable=False)

    raw_json: Mapped[dict | None] = mapped_column(JSON)
    raw_text: Mapped[str | None] = mapped_column(Text)

    fit_score: Mapped[int | None] = mapped_column(Integer)
    fit_class: Mapped[str | None] = mapped_column(Enum("Good", "Maybe", "No"))
    penalty_flags: Mapped[dict | None] = mapped_column(JSON)

    salary_text: Mapped[str | None] = mapped_column(String(512))

    notion_page_id: Mapped[str | None] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(
        Enum("New", "Shortlist", "Applied", "Interview", "Offer", "Rejected", "Accepted"),
        nullable=False,
        default="New",
    )

    notion_last_sync: Mapped[str | None] = mapped_column(DateTime)
    notion_last_error: Mapped[str | None] = mapped_column(Text)

    source: Mapped["Source"] = relationship(back_populates="jobs")
    enrichment: Mapped["JobEnrichment"] = relationship(back_populates="job", uselist=False)


class JobEnrichment(Base):
    __tablename__ = "job_enrichment"

    job_uid: Mapped[str] = mapped_column(String(40), ForeignKey("jobs.job_uid"), primary_key=True)
    summary: Mapped[str | None] = mapped_column(Text)
    skills_json: Mapped[dict | None] = mapped_column(JSON)
    pros: Mapped[str | None] = mapped_column(Text)
    cons: Mapped[str | None] = mapped_column(Text)
    outreach_target: Mapped[str | None] = mapped_column(String(512))

    job: Mapped["Job"] = relationship(back_populates="enrichment")
