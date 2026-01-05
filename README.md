# WHAT IS THIS

Using AI assistants as if they were Senior Python Developers.

My goal here is to transform business needs and logic into programs using AI tools at their full potential.

I'm NOT a developer (just a geek that likes to implement dashboards and alerts to improve Quality of Service and Quality of Life on his job), so see this as a "funny playground".

**IMPORTANT NOTE**: this is personal automation, not production-hardened, no SLA.

# JOB SEARCH BOT

Personal automation to ingest job postings from ATS providers and maintain a local database,
with an optional sync to a Notion database (Notion “data source”).

This project focuses on:
- deterministic ingestion (idempotent upserts)
- predictable caps and rate limiting
- clean operational behavior under systemd (journald logs + failure signaling)
- PEP 8 / pythonic code style and tests for new changes

---

## What it does

1) Ingest job postings from:
- Lever
- Greenhouse (including an optional job-detail call for richer text)

2) Upsert into MySQL tables:
- `sources`
- `jobs`
- `job_enrichment`

3) Enforce runtime caps:
- Daily API calls per provider (DB counters)
- Daily cap for NEW jobs created (DB counter)
- Max number of fetched items processed per run

4) LLM enrichment (optional):
- enriches raw job data into `job_enrichment` (summary, skills, pros/cons, salary inference, etc.)

5) Multi-profile fit scoring (CV-driven, deterministic):
- supports multiple profiles (multi-tenant friendly for personal use)
- bootstraps profiles from CV `.docx`
- computes fit score / fit class / penalty flags per **(profile, job)** using rule-based logic

6) Optionally sync to Notion:
- only jobs above a fit score threshold
- only jobs that changed since last sync (or never synced)
- supports a Notion property `Profile` (text) to avoid collisions

7) Operational integration:
- scheduled execution via systemd timer
- JSON structured logs to journald
- fail-fast configuration validation
- minimal alerting (unit failure handler)

---

## Functional requirements (consolidated)

### ATS ingest
- Supported providers: Lever, Greenhouse
- Each job is uniquely identified by: `provider + company_slug + ats_job_id`
- `job_uid` is a SHA1 hex string (40 chars):
  - `sha1("{ats_type}:{company_slug}:{ats_job_id}")`

### Job detail call (Greenhouse)
- Greenhouse list endpoints can be incomplete.
- The ingest pipeline may perform an additional “job detail” call to fetch richer fields (notably job content),
  improving `raw_text` quality and downstream enrichment/scoring.

### Salary fields
- `jobs.salary_text` = salary text extracted from ATS payload (raw)
- `job_enrichment.salary` = normalized / inferred / enriched salary (LLM enrichment step)

### Rate limits and caps
- Daily API call cap per provider:
  - `MAX_CALLS_PER_DAY`
  - stored in DB counter table (provider-level)
- Daily cap on number of NEW jobs created:
  - `MAX_NEW_JOBS_PER_DAY`
  - only blocks inserts; updates remain allowed
- Per-run processing cap:
  - `MAX_FETCH_PER_RUN`

### Multi-profile (Solution B)
Goal: support multiple “profiles” (different users) that compute different fit scores for the same jobs.

Core requirements:
- CV is stored on disk in a fixed profile path:
  - `{PROFILES_DIR}/{PROFILE_ID}/cv.docx`
- On each run (when multi-profile enabled):
  - compute `cv_sha256`
  - upsert into `profiles`
  - refresh only when hash changes
  - invalidate profile-dependent fit state on hash change

### Fit score “real” per profile (deterministic + refresh logic)
Fit state is stored in `job_profile` (NOT in `jobs` anymore for operational logic):
- `job_profile.fit_score` (0–100)
- `job_profile.fit_class` (“Good”, “Maybe”, “No”)
- `job_profile.penalty_flags` (JSON)

Deterministic invalidation keys:
- `job.last_checked`
- `profile.cv_sha256`

A job is considered “fit-stale” for a profile if:
- no `job_profile` row exists for (job, profile), OR
- `job_profile.fit_job_last_checked != job.last_checked`, OR
- `job_profile.fit_profile_cv_sha256 != profile.cv_sha256`

Scoring approach (rule-based deterministic):
- Skill match primarily from `job_enrichment.skills_json` (LLM enrichment output).
- Fallback skill detection from job title/text (keyword-based).
- Seniority alignment (junior/mid/senior) inferred from CV text + job title/text.
- Language requirements detected conservatively from job text (only when explicitly marked as “required/must/fluent/native”).
- Location constraints applied only when the profile CV contains location hints and the job is not remote.

### Notion sync (optional)
- Controlled by:
  - `SYNC_TO_NOTION=1` to enable
  - `SYNC_TO_NOTION=0` to disable
- Sync eligibility:
  - uses **profile fit score** in multi-profile mode
  - `fit_score >= FIT_MIN`
  - changed since last sync: `last_checked > notion_last_sync` OR never synced
- Sync behavior:
  - ingestion remains successful even if Notion sync fails (failure is logged)
- Multi-profile Notion:
  - Notion DB must have a `Profile` property (type: text)
  - pages are located by `(Job UID, Profile)` to prevent collisions

### systemd execution
- A systemd timer triggers the ingestion script (twice per day in the current setup)
- Logs go to journald
- Unit failure can trigger an alert handler (minimal alerting)

---

## Technical choices

### Stack
- Python 3.13
- SQLAlchemy ORM + PyMySQL
- requests for ATS HTTP calls
- Notion API client for optional sync
- pytest for test execution
- python-docx for CV parsing (`.docx`)

### Database model (high level)
- `sources`: one row per company ATS endpoint
- `jobs`: one row per job posting, keyed by `job_uid` (SHA1)
- `job_enrichment`: enrichment payload and metadata, keyed by `job_uid`
- `profiles`: one row per profile, keyed by `profile_id`, with CV hash + extracted text
- `job_profile`: per-(job,profile) state:
  - fit score/class/flags
  - Notion mapping (page_id + sync timestamps) per profile
  - deterministic staleness columns (`fit_job_last_checked`, `fit_profile_cv_sha256`)

### Rate limiting & caps implementation
- API calls per provider are counted in a DB table and reserved via an atomic update.
- New job creations are capped via a daily DB counter table.
- The ingestion runner can stop early when `MAX_FETCH_PER_RUN` is reached.

### Structured logging
- JSON logs to stdout (systemd/journald friendly)
- Each run has a `run_id` for correlation
- Events are emitted with consistent `event` fields (e.g. `ingest_start`, `ingest_done`, `profile_bootstrap_done`)

### Fail-fast config
- Configuration is validated at startup (`validate_settings`).
- Invalid configuration exits with code 2 (systemd marks the unit as failed).

### Minimal alerting
- A dedicated systemd `OnFailure=` handler can emit an alert to journald and optionally call a webhook.

---

## Configuration

The application loads configuration from environment variables (via `.env`).

### Required MySQL variables
- `MYSQL_HOST`
- `MYSQL_PORT` (default 3306)
- `MYSQL_DB`
- `MYSQL_USER`
- `MYSQL_PASSWORD`

### Notion variables (required only when SYNC_TO_NOTION=1)
- `NOTION_TOKEN`
- `NOTION_DATA_SOURCE_ID`
- `NOTION_VERSION` (default `2025-09-03`)

### Runtime controls
- `SYNC_LIMIT` (default 50)
- `FIT_MIN` (default 60)
- `SYNC_TO_NOTION` (0 or 1, default 1)

### Ingestion caps
- `MAX_CALLS_PER_DAY` (default 50; 0 means unlimited)
- `MAX_NEW_JOBS_PER_DAY` (default 200; 0 means unlimited)
- `MAX_FETCH_PER_RUN` (default 50; must be >= 1)
- `REQUEST_TIMEOUT_S` (default 20)
- `GREENHOUSE_PER_PAGE` (default 100)
- `GREENHOUSE_MAX_PAGES` (default 50)
- `INGEST_PER_SOURCE_LIMIT` (default 0 = unlimited)

### Multi-profile controls (Solution B)
- `PROFILES_DIR` (default empty string = disabled)
  - Example: `/opt/jobs-bot/profiles`
- `PROFILE_ID` (default `default`)
  - CV path convention: `{PROFILES_DIR}/{PROFILE_ID}/cv.docx`

When `PROFILES_DIR` is set, the pipeline will:
- bootstrap/update profile (hash + extracted CV text)
- compute fit scores per profile
- sync Notion pages with `Profile=<PROFILE_ID>` to avoid collisions

---

## How to run

### Local run (manual)
1) Create and activate a venv
2) Install dependencies from `requirements.txt`
3) Configure `.env`
4) Run:
   - `python ingest_run.py`

### systemd run
- Ensure your `jobs-bot-ingest.service` runs `ingest_run.py` within the venv
- Ensure `.env` is loaded via `EnvironmentFile=...`
- Use a timer to schedule execution

To inspect logs:
- `journalctl -u jobs-bot-ingest.service -n 200 --no-pager`

---

## Tests

Run unit/integration tests:
- `pytest -q`

New features must ship with tests to preserve (and ideally improve) coverage.

---

## Operational notes / troubleshooting

### “Sources OK: 0”
This can happen if:
- all sources are rate-limited (not treated as a failure by default), or
- sources are failing (bad endpoint, network, HTTP errors)

Check:
- `sources.last_error`
- journald logs for the run

### Notion sync failures
Notion sync is treated as optional:
- ingestion remains successful even if Notion fails
- failures are logged with `event=notion_sync_failed`

### Profile bootstrap failures
Common causes:
- CV file missing at `{PROFILES_DIR}/{PROFILE_ID}/cv.docx`
- invalid/unreadable `.docx` format

Failures are logged with `event=profile_bootstrap_failed` and the unit exits non-zero.

---

## Planned / next steps (design notes)

### CV → profile_json via LLM
When needed, add an LLM step that transforms `profile_text` into structured `profile_json`
to improve fit scoring and penalties with more semantic signals.

This is intentionally deferred to keep the system deterministic and low-risk.
