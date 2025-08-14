## BigQuery Syncer (MySQL → BigQuery)

A production-grade ETL pipeline to sync MySQL tables into BigQuery daily with incremental loads, optional backfills, schema validation, retries, and structured logging.

### Features

- Incremental extract by `updated_at` or auto-increment key
- Transform for BigQuery-compatible schema (types, nulls, timestamps)
- Load with append or upsert (MERGE) via staging tables
- Persistent state (local JSON or GCS)
- Schema validation and optional automatic column additions
- Retries with exponential backoff
- Structured logging and daily run reports
- Daily scheduling via APScheduler (or use cron/Airflow)

### Setup

1. Python 3.10+
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `env.example` to `.env` and fill values:

```bash
cp env.example .env
```

4. Update `config.yaml` (dataset, mappings, options).

For GCP auth, set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON with BigQuery and (optional) GCS permissions.

### Configuration

- `config.yaml` defines project, dataset, and table mappings.
- Environment variables supply secrets (`MYSQL_*`, `GOOGLE_APPLICATION_CREDENTIALS`).

### Run Locally (one-off)

```bash
python -m etl.main run-once
```

### Schedule Daily

- In-code scheduler (APScheduler):

```bash
python -m etl.main schedule
```

- Or use cron:

```bash
0 2 * * * cd /path/to/bigquery-syncer && /usr/bin/python3 -m etl.main run-once >> sync.log 2>&1
```

### Backfill

Provide `backfill_start` in `config.yaml` for a mapping, or run:

```bash
python -m etl.main backfill --mapping users --start 2020-01-01T00:00:00Z --end 2020-12-31T23:59:59Z
```

### Troubleshooting

- Verify network egress to MySQL and GCP.
- Check env variables and service account permissions.
- For partial failures, re-run the same job; idempotent upserts avoid duplicates.
- Increase log verbosity with `LOG_LEVEL=DEBUG`.

### Deploying

- VM: Install Python, copy repo, set `.env`, run with cron.
- Cloud Run: Wrap `run-once` in a container and trigger via Cloud Scheduler.
- Airflow: Call `etl.main:run_once` from a PythonOperator or use `BashOperator`.

### Structure

```
etl/
  __init__.py
  config_loader.py
  logger.py
  state.py
  schema.py
  extract.py
  transform.py
  load.py
  report.py
  scheduler.py
  main.py
config.yaml
```
