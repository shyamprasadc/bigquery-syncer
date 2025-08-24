# BigQuery Syncer (MySQL → BigQuery)

A production-grade ETL pipeline to sync MySQL tables into BigQuery daily with incremental loads, optional backfills, schema validation, retries, and structured logging.

## Features

### Core ETL Pipeline

- **Incremental Extract**: Fetch data by `updated_at` timestamp or auto-increment primary key with configurable operators (`>`, `>=`)
- **Transform**: Handle BigQuery-compatible schema conversion (types, nulls, timestamps with timezone normalization)
- **Load**: Support both append and upsert (MERGE) modes via temporary staging tables
- **Batch Processing**: Automatic pagination for large datasets to prevent memory issues

### Data Management

- **Persistent State**: Track sync progress in local JSON files or Google Cloud Storage
- **Schema Validation**: Compare MySQL and BigQuery schemas with optional automatic column additions
- **Watermark Tracking**: Safe incremental syncs with precise cursor advancement

### Reliability & Monitoring

- **Retry Logic**: Exponential backoff for MySQL connections and queries (via tenacity)
- **Error Handling**: Graceful partial failures - one mapping failure doesn't stop others
- **Structured Logging**: Context-aware logging with mapping and operation details
- **Daily Reports**: JSON reports with processing statistics saved to `etl/reports/report.json`

### Scheduling & Operations

- **Flexible Scheduling**: APScheduler integration or external cron/Airflow support
- **CLI Interface**: Commands for one-off runs, scheduling, and historical backfills
- **Configuration**: YAML-based config with environment variable interpolation (`${ENV_VAR}`)

### Infrastructure

- **Auto-creation**: Automatically creates BigQuery datasets and tables if they don't exist
- **GCS Integration**: Optional Google Cloud Storage for state persistence and staging
- **Idempotent Operations**: Safe to re-run without data duplication

## Setup

### Prerequisites

- Python 3.10+
- MySQL database with network access
- Google Cloud project with BigQuery API enabled
- Service account with BigQuery permissions (and GCS if using cloud state)

### Installation

1. **Install dependencies**:

```bash
pip install -r requirements.txt
```

2. **Configure environment**:

```bash
cp env.example .env
# Edit .env with your MySQL and GCP credentials
```

3. **Update configuration**:
   Edit `config.yaml` with your project details and table mappings.

### Environment Variables

Required:

- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)
- `GCP_PROJECT_ID`

Optional:

- `GCS_STATE_BUCKET`, `GCS_STATE_PREFIX` (for cloud state storage)
- `GCS_TEMP_BUCKET` (for staging tables)
- `LOG_LEVEL` (default: INFO)

## Configuration

The `config.yaml` file defines:

- **Project settings**: GCP project, dataset, location
- **State management**: Local vs GCS storage options
- **Schema handling**: Whether to auto-add missing columns
- **Scheduling**: Cron expression for APScheduler mode
- **Table mappings**: Source MySQL tables to destination BigQuery tables

### Mapping Configuration

Each mapping defines:

- `name`: Unique identifier for the mapping
- `mysql_table`: Source table name
- `bigquery_table`: Destination table name
- `primary_keys`: List of primary key columns (required for upsert mode)
- `incremental_column`: Column to use for incremental sync (`updated_at`, `id`, etc.)
- `mode`: `append` or `upsert`
- `backfill_start`: Optional ISO timestamp for historical backfill

## Usage

### One-off Run

```bash
python -m etl.main run-once
```

### Scheduled Execution

**APScheduler (built-in)**:

```bash
python -m etl.main schedule
```

**External cron**:

```bash
0 2 * * * cd /path/to/bigquery-syncer && /usr/bin/python3 -m etl.main run-once >> sync.log 2>&1
```

### Historical Backfill

```bash
# Backfill specific mapping with date range
python -m etl.main backfill --mapping users --start 2020-01-01T00:00:00Z --end 2020-12-31T23:59:59Z

# Or configure backfill_start in config.yaml for automatic backfill
```

## Monitoring & Troubleshooting

### Logs

- Structured logs include mapping context and operation details
- Set `LOG_LEVEL=DEBUG` for verbose output
- Logs show processing statistics and error details

### Reports

- Daily run reports saved to `etl/reports/report.json`
- Include records processed, duration, and any errors per mapping

### Common Issues

- **Network connectivity**: Verify MySQL and GCP network access
- **Authentication**: Check service account permissions and credentials
- **Schema mismatches**: Review schema validation warnings in logs
- **Partial failures**: Re-run safely - operations are idempotent

## Deployment Options

### Virtual Machine

1. Install Python and dependencies
2. Copy repository and configuration files
3. Set up `.env` with production credentials
4. Use system cron for scheduling

### Google Cloud Run

1. Containerize the application
2. Set environment variables in Cloud Run
3. Use Cloud Scheduler to trigger `run-once` command

### Apache Airflow

```python
from etl.main import run_once

# In your DAG
sync_task = PythonOperator(
    task_id='mysql_to_bigquery_sync',
    python_callable=run_once,
    op_kwargs={'config_path': 'config.yaml'}
)
```

## Project Structure

```
bigquery-syncer/
├── etl/
│   ├── __init__.py
│   ├── config_loader.py    # YAML config with env interpolation
│   ├── logger.py           # Structured logging
│   ├── state.py            # Persistent state (local/GCS)
│   ├── schema.py           # Schema validation & auto-addition
│   ├── extract.py          # MySQL incremental extraction
│   ├── transform.py        # Data transformation & normalization
│   ├── load.py             # BigQuery loading (append/upsert)
│   ├── report.py           # Run reporting
│   ├── scheduler.py        # APScheduler integration
│   ├── main.py             # CLI entrypoint
│   └── reports/            # Daily run reports
├── config.yaml             # Main configuration
├── env.example             # Environment template
├── requirements.txt        # Python dependencies
└── README.md
```

## Development

### Adding New Features

- Follow the modular structure in `etl/`
- Add comprehensive error handling
- Include logging with appropriate context
- Update configuration schema as needed

### Testing

- Test with small datasets first
- Verify incremental sync behavior
- Check schema validation with new column types
- Validate upsert operations with primary key conflicts
