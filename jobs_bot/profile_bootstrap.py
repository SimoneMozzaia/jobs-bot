from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
from typing import Optional

from docx import Document
from sqlalchemy import update
from sqlalchemy.orm import Session

from .models import JobProfile, Profile


class ProfileBootstrapError(RuntimeError):
    """Raised when profile bootstrap fails in a way that should stop the run."""


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_docx_text(path: Path) -> str:
    """Extract deterministic plain text from a .docx file."""
    doc = Document(str(path))
    parts: list[str] = []
    for p in doc.paragraphs:
        txt = (p.text or "").strip()
        if txt:
            parts.append(txt)
    return "\n".join(parts).strip()


def bootstrap_profile(
    session: Session,
    *,
    profile_id: str,
    cv_path: str,
    now: Optional[dt.datetime] = None,
) -> tuple[Profile, bool]:
    """Create or refresh a profile record from a CV file.

    Returns:
        (profile, changed): changed=True when the profile was created or its cv_sha256 changed.
    """
    if not profile_id or not profile_id.strip():
        raise ProfileBootstrapError("profile_id is required")

    cv = Path(cv_path)
    if not cv.exists() or not cv.is_file():
        raise ProfileBootstrapError(f"CV file not found: {cv}")

    now = now or dt.datetime.now(dt.UTC).replace(tzinfo=None)

    cv_sha256 = _sha256_file(cv)

    existing = session.get(Profile, profile_id)

    try:
        profile_text = _extract_docx_text(cv)
    except Exception as exc:
        # Best-effort persist error for debugging, then fail fast.
        if existing is not None:
            existing.last_error = str(exc)
            session.commit()
        raise ProfileBootstrapError(f"Failed to parse CV .docx: {exc}") from exc

    changed = False
    if existing is None:
        profile = Profile(
            profile_id=profile_id,
            cv_path=str(cv),
            cv_sha256=cv_sha256,
            profile_json=None,
            profile_text=profile_text,
            analyzed_at=None,
            last_error=None,
        )
        session.add(profile)
        changed = True
    else:
        profile = existing
        if (profile.cv_sha256 or "") != cv_sha256 or (profile.cv_path or "") != str(cv):
            profile.cv_path = str(cv)
            profile.cv_sha256 = cv_sha256
            profile.profile_text = profile_text
            profile.profile_json = None
            profile.analyzed_at = None
            profile.last_error = None
            changed = True

    if changed:
        # Deterministic invalidation: reset fit fields so scoring will recompute.
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
                # Keep notion_page_id to avoid collisions, but clear sync markers.
                notion_last_sync=None,
                notion_last_error=None,
            )
        )

    session.commit()
    return profile, changed
