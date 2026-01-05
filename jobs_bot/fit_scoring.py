from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import and_, or_, select

from .api_usage import utcnow_naive
from .models import Job, JobEnrichment, JobProfile, Profile


_SENIORITY_WORDS = (
    "senior",
    "lead",
    "principal",
    "staff",
    "manager",
    "head",
    "director",
)


def _fit_class(score: int) -> str:
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Maybe"
    return "No"


def _has_seniority(title: str) -> bool:
    t = (title or "").lower()
    return any(w in t for w in _SENIORITY_WORDS)


def _profile_mentions_seniority(profile_text: str) -> bool:
    t = (profile_text or "").lower()
    return any(w in t for w in _SENIORITY_WORDS)


def _score_job(profile: Profile, job: Job, enrich: JobEnrichment | None) -> tuple[int, str, dict | None]:
    profile_text = (profile.profile_text or "").lower()

    skills: list[str] = []
    if enrich and isinstance(enrich.skills_json, dict):
        raw = enrich.skills_json.get("skills")
        if isinstance(raw, list):
            skills = [str(s).strip() for s in raw if s]

    matched: list[str] = []
    missing: list[str] = []

    for s in skills:
        token = s.lower()
        if token and token in profile_text:
            matched.append(s)
        else:
            missing.append(s)

    penalty_flags: dict = {
        "skills_total": len(skills),
        "skills_matched": len(matched),
    }

    if missing:
        penalty_flags["missing_skills"] = missing[:50]

    if skills:
        ratio = len(matched) / max(1, len(skills))
        score = int(round(ratio * 80))
    else:
        score = 40
        penalty_flags["no_skills_list"] = True

    title = job.title or ""
    if any(k in title.lower() for k in matched):
        score += 5

    if _has_seniority(title) and not _profile_mentions_seniority(profile.profile_text or ""):
        score -= 15
        penalty_flags["seniority_mismatch"] = True

    score = max(0, min(100, score))
    fit_class = _fit_class(score)
    return score, fit_class, penalty_flags or None


@dataclass(frozen=True)
class FitScoreStats:
    attempted: int
    inserted_jobs: int
    updated_jobs: int
    skipped_up_to_date: int


def compute_fit_scores_for_profile(
    session,
    *,
    profile: Profile,
    limit: int = 200,
) -> FitScoreStats:
    """Compute/update fit scores for a profile for jobs that are out-of-date.

    Out-of-date condition (deterministic):
      - JobProfile missing
      - JobProfile.fit_job_last_checked != Job.last_checked
      - JobProfile.fit_profile_cv_sha256 != Profile.cv_sha256
    """
    now = utcnow_naive()

    stmt = (
        select(Job, JobProfile, JobEnrichment)
        .join(JobEnrichment, JobEnrichment.job_uid == Job.job_uid, isouter=True)
        .join(
            JobProfile,
            and_(
                JobProfile.job_uid == Job.job_uid,
                JobProfile.profile_id == profile.profile_id,
            ),
            isouter=True,
        )
        .where(
            or_(
                JobProfile.job_uid.is_(None),
                JobProfile.fit_job_last_checked.is_(None),
                JobProfile.fit_job_last_checked != Job.last_checked,
                JobProfile.fit_profile_cv_sha256.is_(None),
                JobProfile.fit_profile_cv_sha256 != profile.cv_sha256,
            )
        )
        .order_by(Job.last_seen.desc())
        .limit(limit)
    )

    rows = session.execute(stmt).all()

    attempted = 0
    inserted = 0
    updated = 0

    for job, job_profile, enrich in rows:
        attempted += 1

        if job_profile is None:
            job_profile = JobProfile(job_uid=job.job_uid, profile_id=profile.profile_id)
            session.add(job_profile)
            inserted += 1
        else:
            updated += 1

        score, cls, penalties = _score_job(profile, job, enrich)

        job_profile.fit_score = score
        job_profile.fit_class = cls
        job_profile.penalty_flags = penalties
        job_profile.fit_job_last_checked = job.last_checked
        job_profile.fit_profile_cv_sha256 = profile.cv_sha256
        job_profile.fit_computed_at = now

    session.commit()

    return FitScoreStats(
        attempted=attempted,
        inserted_jobs=inserted,
        updated_jobs=updated,
        skipped_up_to_date=0,
    )
