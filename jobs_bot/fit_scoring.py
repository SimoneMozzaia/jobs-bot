# jobs_bot/fit_scoring.py
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, aliased

from .models import Job, JobEnrichment, JobProfile, Profile


@dataclass(frozen=True)
class FitScoreStats:
    attempted: int
    inserted_jobs: int
    updated_jobs: int
    skipped_jobs: int = 0


_LANG_ALIASES: dict[str, tuple[str, ...]] = {
    "english": ("english", "inglese"),
    "italian": ("italian", "italiano"),
    "french": ("french", "français", "francese"),
    "german": ("german", "deutsch", "tedesco"),
    "spanish": ("spanish", "español", "spagnolo"),
    "portuguese": ("portuguese", "português", "portoghese"),
}

_LANGUAGE_MARKERS: tuple[str, ...] = (
    "required",
    "must",
    "mandatory",
    "fluent",
    "native",
    "needed",
)

_LEVEL_ORDER: dict[str, int] = {
    "unknown": 0,
    "junior": 1,
    "mid": 2,
    "senior": 3,
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _extract_skills_from_enrichment(enrich: JobEnrichment | None) -> list[str]:
    if not enrich or not enrich.skills_json or not isinstance(enrich.skills_json, dict):
        return []
    raw = enrich.skills_json.get("skills")
    if not isinstance(raw, list):
        return []
    skills = [str(x).strip() for x in raw if x]
    return [s for s in skills if s]


def _profile_languages(profile_blob: str) -> set[str]:
    out: set[str] = set()
    for canon, aliases in _LANG_ALIASES.items():
        if any(a in profile_blob for a in aliases):
            out.add(canon)
    return out


def _extract_required_languages(job_blob: str) -> set[str]:
    """
    Detect languages explicitly required by the job text.

    Supports both patterns:
      - "<marker> ... <language>"  (e.g. "Fluent Italian ...")
      - "<language> ... <marker>"  (e.g. "Italian is required")
    """
    out: set[str] = set()
    markers = "|".join(_LANGUAGE_MARKERS)

    for canon, aliases in _LANG_ALIASES.items():
        for alias in aliases:
            if alias not in job_blob:
                continue

            # marker -> language
            p1 = rf"\b(?:{markers})\b[^\n]{{0,50}}\b{re.escape(alias)}\b"
            # language -> marker
            p2 = rf"\b{re.escape(alias)}\b[^\n]{{0,50}}\b(?:{markers})\b"

            if re.search(p1, job_blob) or re.search(p2, job_blob):
                out.add(canon)
                break

    return out


def _max_years_experience(profile_blob: str) -> int | None:
    """
    Extract a rough max 'years of experience' signal from text.

    Examples supported:
      - "1 year of experience"
      - "5 years experience"
      - "3 yrs of experience"
      - "10+ years"
    """
    patterns = (
        r"\b(\d{1,2})\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?experience\b",
        r"\b(\d{1,2})\s*\+?\s*(?:years?|yrs?)\s+experience\b",
        r"\b(\d{1,2})\s*\+?\s*(?:years?|yrs?)\b",
    )
    for pat in patterns:
        m = re.search(pat, profile_blob)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def _infer_profile_seniority(profile_blob: str) -> str:
    if any(w in profile_blob for w in ("principal", "staff", "lead", "senior")):
        return "senior"
    if any(w in profile_blob for w in ("junior", "entry", "graduate", "intern")):
        return "junior"

    years = _max_years_experience(profile_blob)
    if years is None:
        return "unknown"
    if years <= 2:
        return "junior"
    if years <= 5:
        return "mid"
    return "senior"


def _infer_job_seniority(job_title: str, job_blob: str) -> str:
    blob = _norm(f"{job_title} {job_blob}")
    if any(w in blob for w in ("principal", "staff", "lead", "senior")):
        return "senior"
    if any(w in blob for w in ("junior", "entry", "graduate", "intern")):
        return "junior"
    return "unknown"


def _contains_skill(profile_blob: str, skill: str) -> bool:
    s = _norm(skill)
    if not s:
        return False
    return s in profile_blob


def _score_job(
    *,
    job: Job,
    enrich: JobEnrichment | None,
    profile: Profile,
) -> tuple[int, str, dict[str, Any] | None]:
    """
    Deterministic rule-based scoring.

    Primary signal:
      - overlap between job required skills (from enrichment.skills_json) and profile_text

    Penalties:
      - missing required language
      - seniority mismatch (job requires higher seniority than profile signal)
      - (location constraints can be added here; kept conservative unless profile expresses strict needs)
    """
    profile_blob = _norm(profile.profile_text or "")
    job_blob = _norm(job.raw_text or "")

    required_skills = _extract_skills_from_enrichment(enrich)
    required_norm = [_norm(s) for s in required_skills if _norm(s)]

    hits = sum(1 for s in required_norm if _contains_skill(profile_blob, s))
    total = len(required_norm)
    ratio = (hits / total) if total > 0 else 0.0

    # Base score: make "perfect match" clearly above 80.
    # total>0 & ratio==1 -> 90
    base_score = int(round(40 + (50 * ratio))) if total > 0 else 50

    penalty_flags: dict[str, Any] = {}

    # Skills diagnostics (only store if something is missing)
    if total > 0 and hits < total:
        missing = [s for s in required_skills if not _contains_skill(profile_blob, s)]
        if missing:
            penalty_flags["missing_skills"] = missing

    # Language penalty
    required_langs = _extract_required_languages(job_blob)
    known_langs = _profile_languages(profile_blob)
    missing_langs = sorted(required_langs - known_langs)
    if missing_langs:
        penalty_flags["missing_required_languages"] = missing_langs
        base_score -= 35

    # Seniority penalty (only if both sides are known)
    job_level = _infer_job_seniority(job.title, job_blob)
    profile_level = _infer_profile_seniority(profile_blob)
    job_v = _LEVEL_ORDER.get(job_level, 0)
    prof_v = _LEVEL_ORDER.get(profile_level, 0)

    if job_v > 0 and prof_v > 0:
        diff = job_v - prof_v
        if diff >= 1:
            penalty_flags["seniority_mismatch"] = {
                "job": job_level,
                "profile": profile_level,
                "diff": diff,
            }
            base_score -= 20 if diff == 1 else 35

    # Clamp
    score = max(0, min(100, int(base_score)))

    fit_class = "No"
    if score >= 75:
        fit_class = "Good"
    elif score >= 60:
        fit_class = "Maybe"

    return score, fit_class, (penalty_flags or None)


def compute_fit_scores_for_profile(
    session: Session,
    *,
    profile: Profile,
    limit: int = 200,
) -> FitScoreStats:
    """
    Compute deterministic fit scores for a single profile.

    Writes into job_profile and uses staleness keys:
      - job.last_checked
      - profile.cv_sha256
    """
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    profile_id = profile.profile_id

    JP = aliased(JobProfile)

    stmt = (
        select(Job, JobEnrichment, JP)
        .join(JobEnrichment, JobEnrichment.job_uid == Job.job_uid, isouter=True)
        .join(
            JP,
            and_(JP.job_uid == Job.job_uid, JP.profile_id == profile_id),
            isouter=True,
        )
        .where(
            or_(
                JP.job_uid.is_(None),
                JP.fit_job_last_checked.is_(None),
                JP.fit_job_last_checked != Job.last_checked,
                JP.fit_profile_cv_sha256.is_(None),
                JP.fit_profile_cv_sha256 != profile.cv_sha256,
            )
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    attempted = 0
    inserted = 0
    updated = 0

    for job, enrich, jp in session.execute(stmt).all():
        attempted += 1

        score, fit_class, penalties = _score_job(job=job, enrich=enrich, profile=profile)

        if jp is None:
            jp = JobProfile(
                job_uid=job.job_uid,
                profile_id=profile_id,
            )
            session.add(jp)
            inserted += 1
        else:
            updated += 1

        jp.fit_score = score
        jp.fit_class = fit_class
        jp.penalty_flags = penalties
        jp.fit_job_last_checked = job.last_checked
        jp.fit_profile_cv_sha256 = profile.cv_sha256
        jp.fit_computed_at = now

    session.commit()

    return FitScoreStats(
        attempted=attempted,
        inserted_jobs=inserted,
        updated_jobs=updated,
        skipped_jobs=0,
    )
