from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import update
from sqlalchemy.orm import Session

from .api_usage import utcnow_naive
from .models import JobProfile, Profile


def sha256_file(path: Path) -> str:
    """Return the SHA256 hex digest of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def bootstrap_profile(
    session: Session,
    *,
    profile_id: str,
    cv_path: str,
) -> tuple[Profile, bool]:
    """
    Ensure the current profile row exists and is up to date.

    Responsibilities:
      - compute CV SHA256
      - upsert into `profiles`
      - if CV hash changed, invalidate profile-derived state in `job_profile`

    Returns:
      (profile, changed)

    Notes:
      - Invalidation keeps Notion page IDs (to avoid duplicate pages) but resets
        fit fields + sync markers so the next scoring/sync can refresh outputs.
    """
    path = Path(cv_path)
    if not path.exists():
        raise FileNotFoundError(f"CV not found: {cv_path}")
    if not path.is_file():
        raise RuntimeError(f"CV path is not a file: {cv_path}")

    now = utcnow_naive()
    digest = sha256_file(path)

    profile = session.get(Profile, profile_id)
    changed = False

    if profile is None:
        profile = Profile(
            profile_id=profile_id,
            cv_path=str(path),
            cv_sha256=digest,
            profile_json=None,
            profile_text=None,
            analyzed_at=None,
            last_error=None,
        )
        session.add(profile)
        changed = True
        session.commit()
        return profile, changed

    # Keep the canonical path in DB.
    profile.cv_path = str(path)

    if profile.cv_sha256 != digest:
        changed = True
        profile.cv_sha256 = digest

        # Future step: LLM CV->profile_json/profile_text.
        profile.profile_json = None
        profile.profile_text = None
        profile.analyzed_at = None
        profile.last_error = None

        # Invalidate profile-dependent per-job state but keep notion_page_id.
        session.execute(
            update(JobProfile)
            .where(JobProfile.profile_id == profile_id)
            .values(
                fit_score=0,
                fit_class="No",
                penalty_flags=None,
                notion_last_sync=None,
                notion_last_error=None,
            )
        )

    session.commit()
    return profile, changed
