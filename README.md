# JOB SEARCH BOT

Using AI assistants as if they were Senior Python Developers. 

My goal here is to transform business needs and logic into programs using AI tools at their full potential.

I'm NOT a developer (just a geek that likes to implement dashboards and alerts to improve the Quality of Service and Quality of Life on his job), so see this as a "funny playground".

**IMPORTANT NOTE** this is **NOT** a production code!

Automation for:
- ingesting job postings from ATS (Greenhouse + Lever)
- upserting to MySQL (`sources`, `jobs`, `job_enrichment` tables)
- daily rate limit per provider (DB counter)
- daily cap on the number of **new jobs entered**
- optional sync to Notion (data source)

## Functional requirements (consolidated)

### ATS ingest
- Supported providers: **Lever**, **Greenhouse**
- Each job is uniquely identified by: `provider + company_slug + ats_job_id`
  - `job_uid` = hexadecimal SHA1 (40 char) of `“{ats_type}:{company_slug}:{ats_job_id}”`

### Salary data and fields
- `jobs.salary_text` = data **extracted from ATS** (raw)
- `job_enrichment.salary` = **normalized/inferred/enriched** data (in subsequent steps)

### Rate limits and caps
- Daily cap per provider (DB): `MAX_CALLS_PER_DAY`
- Daily cap for new jobs (DB): `MAX_NEW_JOBS_PER_DAY` (inserts only, no updates)
- Maximum cap for “fetch items” per run: `MAX_FETCH_PER_RUN`

### Notion sync (optional)
- Enable: `SYNC_TO_NOTION=1`
- Sync eligible jobs:
  - `fit_score >= FIT_MIN`
  - jobs with changes since last sync (`last_checked > notion_last_sync`) or never synced

### systemd
- Scheduled execution via timer+service
- Output to journald (stdout)

---

## Technical choices

### Stack
- Python 3.13
- SQLAlchemy ORM + PyMySQL
- requests for ATS API
- Notion API for sync

### Structured logging
- Log in 
