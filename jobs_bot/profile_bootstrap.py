from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import update
from sqlalchemy.orm import Session

from .api_usage import utcnow_naive
from .cv_reader import read_docx_text
from .models import JobProfile, Profile


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def bootstrap_profile(session: Session, *, profile_id: str, cv_path: str) -> tuple[Profile, bool]:
    """Create or refresh a Profile row based on the CV on disk.

    Returns:
        (profile, changed)

    Behavior:
        - compute cv_sha256
        - read CV text from docx and store profile_text
        - upsert Profile
        - if hash changed, invalidate per-job fit state (JobProfile)
    """
    path = Path(cv_path)
    if not path.exists():
        raise RuntimeError(f"CV file not found: {path}")

    now = utcnow_naive()
    digest = sha256_file(path)
    cv_text = read_docx_text(path)

    profile = session.get(Profile, profile_id)
    changed = False

    if profile is None:
        profile = Profile(
            profile_id=profile_id,
            cv_path=str(path),
            cv_sha256=digest,
            profile_text=cv_text,
            analyzed_at=None,
            profile_json=None,
            last_error=None,
        )
        session.add(profile)
        session.commit()
        return profile, True

    if profile.cv_sha256 != digest or profile.cv_path != str(path):
        changed = True
        profile.cv_sha256 = digest
        profile.cv_path = str(path)
        profile.profile_text = cv_text

        # In future: CV->profile_json via LLM; for now reset analysis cache on change.
        profile.profile_json = None
        profile.analyzed_at = None
        profile.last_error = None

        session.execute(
            update(JobProfile)
            .where(JobProfile.profile_id == profile_id)
            .values(
                fit_score=0,
                fit_class="No",
                penalty_flags=None,
                fit_job_last_checked=None,
                fit_profile_cv_sha256=None,
                fit_computed_at=None,
                notion_last_sync=None,
                notion_last_error=None,
            )
        )

    session.commit()
    return profile, changed
