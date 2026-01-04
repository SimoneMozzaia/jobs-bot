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

## What it does

1) Ingest job postings from:
- Lever
- Greenhouse

2) Upsert into MySQL tables:
- `sources`
- `jobs`
- `job_enrichment`

3) Enforce runtime caps:
- Daily API calls per provider (DB counters)
- Daily cap for NEW jobs created (DB counter)
- Max number of fetched items processed per run

4) Optionally sync to Notion:
- Only jobs above a fit score threshold
- Only jobs that changed since last sync (or never synced)

5) Operational integration:
- Scheduled execution via systemd timer
- JSON structured logs to journald
- Fail-fast configuration validation
- Minimal alerting (unit failure handler)

---

## Functional requirements (consolidated)

### ATS ingest
- Supported providers: Lever, Greenhouse
- Each job is uniquely identified by: `provider + company_slug + ats_job_id`
- `job_uid` is a SHA1 hex string (40 chars):
  - `sha1("{ats_type}:{company_slug}:{ats_job_id}")`

### Salary fields
- `jobs.salary_text` = salary text extracted from ATS payload (raw)
- `job_enrichment.salary` = normalized / inferred / enriched salary (enrichment step)

### Rate limits and caps
- Daily API call cap per provider:
  - `MAX_CALLS_PER_DAY`
  - Stored in DB counter table (provider-level)
- Daily cap on number of NEW jobs created:
  - `MAX_NEW_JOBS_PER_DAY`
  - Only blocks inserts; updates remain allowed
- Per-run processing cap:
  - `MAX_FETCH_PER_RUN`

### Notion sync (optional)
- Controlled by:
  - `SYNC_TO_NOTION=1` to enable
  - `SYNC_TO_NOTION=0` to disable
- Sync eligibility:
  - `fit_score >= FIT_MIN`
  - changed since last sync: `last_checked > notion_last_sync` OR never synced
- Sync behavior:
  - Ingestion remains successful even if Notion sync fails (failure is logged)

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

### Database model (high level)
- `sources`: one row per company ATS endpoint
- `jobs`: one row per job posting, keyed by `job_uid` (SHA1)
- `job_enrichment`: enrichment payload and metadata, keyed by `job_uid`

### Rate limiting & caps implementation
- API calls per provider are counted in a DB table and reserved via an atomic update.
- New job creations are capped via a daily DB counter table.
- The ingestion runner can stop early when `MAX_FETCH_PER_RUN` is reached.

### Structured logging
- JSON logs to stdout (systemd/journald friendly)
- Each run has a `run_id` for correlation
- Events are emitted with consistent `event` fields (e.g. `ingest_start`, `ingest_done`, `notion_sync_done`)

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

---

## Planned / next steps (design notes)

### Multi-profile fit scoring (CV-driven)
Goal: support multiple “profiles” (different users) that compute different fit scores for the same jobs.

High-level idea:
- Store a CV in a fixed profile path (e.g. `/opt/jobs-bot/profiles/<profile_key>/cv.docx`)
- Compute `cv_sha256` and only re-analyze the profile if the hash changes
- Persist profile analysis in DB (structured JSON)
- Compute fit score / fit class / penalty flags **per (profile, job)**, not globally in `jobs`

Expected additions (DB-level):
- `profiles` table (profile_key, cv_path, cv_sha256, profile_json, analyzed_at, last_error, etc.)
- `job_fit` (or `job_profile_fit`) table keyed by `(job_uid, profile_id)` containing fit score, fit class, penalties, computed_at

Operational approach:
- Keep ingestion global (avoid duplicate API calls and wasted rate-limit budget)
- Run scoring + Notion sync per profile (systemd template units recommended)

Security note:
- This remains personal automation and not fully hardened for multi-tenant use cases.
