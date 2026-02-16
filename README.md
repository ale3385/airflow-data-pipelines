# Airflow Data Pipelines

Production-ready Airflow DAGs for healthcare data engineering on AWS and Snowflake. Demonstrates real-world pipeline patterns including EMR Serverless batch processing, S3-to-Snowflake ingestion, dbt orchestration, and API data pulls.

Built for [Astronomer](https://www.astronomer.io/) (managed Airflow).

## Architecture

```
                    ┌──────────────────────────────────┐
                    │         Airflow (Astronomer)      │
                    │                                   │
                    │  ┌─────────┐  ┌───────────────┐  │
   APIs ──────────▶ │  │   API   │  │  Snowflake    │  │
                    │  │ Ingest  │  │  S3 Ingest    │  │
                    │  └────┬────┘  └───────┬───────┘  │
                    │       │               │          │
   S3 Landing ────▶ │       ▼               ▼          │
                    │  ┌─────────────────────────┐     │
                    │  │      Snowflake RAW      │     │
                    │  └────────────┬────────────┘     │
                    │               │                   │
                    │  ┌────────────▼────────────┐     │
   S3 Raw ────────▶ │  │   EMR Serverless        │     │
                    │  │   (PySpark Processing)   │     │
                    │  └────────────┬────────────┘     │
                    │               │                   │
                    │  ┌────────────▼────────────┐     │
                    │  │   dbt Orchestration      │     │
                    │  │   staging → marts        │     │
                    │  └─────────────────────────┘     │
                    └──────────────────────────────────┘
```

## DAGs

### `claims_emr_batch_processing`
Heavy batch processing using EMR Serverless (PySpark). Full lifecycle:
1. Discover new files in S3
2. Upload PySpark script to S3
3. Create EMR Serverless application
4. Submit and monitor Spark job
5. COPY INTO Snowflake
6. Cleanup (S3 output + EMR app)

### `snowflake_s3_ingestion`
Multi-source S3 → Snowflake ingestion using COPY INTO. Processes providers, eligibility, and pharmacy data with file format handling (CSV, Parquet, JSON) and post-load validation.

### `dbt_daily_run`
Staged dbt execution: deps → seed → snapshot → staging (run + test) → marts (run + test). Includes source freshness checks as a parallel task.

### `api_vendor_ingestion`
Paginated API extraction → S3 staging (NDJSON) → Snowflake COPY INTO. Handles pagination, empty responses (skip), and temporary file management.

## Project Structure

```
├── dags/
│   ├── emr_batch_processing.py     # EMR Serverless pipeline
│   ├── snowflake_ingestion.py      # S3 → Snowflake COPY INTO
│   ├── dbt_orchestration.py        # dbt staged execution
│   └── api_ingestion.py            # API → S3 → Snowflake
├── utils/
│   ├── aws_helpers.py              # S3 operations, session management
│   ├── emr_helpers.py              # EMR Serverless lifecycle
│   ├── snowflake_helpers.py        # Query execution, COPY INTO
│   └── dbt_helpers.py              # dbt CLI wrapper
├── spark_jobs/
│   └── process_claims.py           # PySpark transformation job
├── sql/
│   └── create_raw_tables.sql       # Snowflake DDL + file formats
├── .github/workflows/ci.yml        # Lint + compile checks
├── .pre-commit-config.yaml         # black, flake8, isort
├── Dockerfile                      # Astronomer runtime
└── requirements.txt
```

## Patterns & Conventions

### TaskFlow API
All DAGs use Airflow's `@task` decorator (TaskFlow API) for clean Python-native task definitions with automatic XCom serialization.

### Graceful Skipping
Tasks that find no data raise `AirflowSkipException` instead of failing, keeping DAG runs green when there's nothing to process.

### Environment Awareness
All helpers use `ENVIRONMENT` env var (`DEV`/`PROD`) to route to the correct Snowflake database (`fl_data_dev`/`fl_data_prod`) and S3 paths.

### Reusable Helpers
The `utils/` module provides shared functionality across DAGs:
- **aws_helpers** — S3 listing, uploads, prefix deletion, session management
- **emr_helpers** — Full EMR Serverless lifecycle (create → submit → wait → delete)
- **snowflake_helpers** — Query execution, COPY INTO, table existence checks
- **dbt_helpers** — CLI wrapper with select/exclude/full-refresh support

### EMR Serverless Lifecycle
Applications are created per-run and auto-deleted after completion to minimize costs. Auto-stop configured at 5 minutes idle.

## Setup

```bash
# Clone
git clone https://github.com/ale3385/airflow-data-pipelines.git
cd airflow-data-pipelines

# Copy and configure environment
cp .env.example .env

# Run locally with Astronomer
astro dev start

# Or with Docker Compose
docker compose up -d
```

## CI/CD

Pull requests run automated checks:
- **black** — Code formatting
- **isort** — Import ordering
- **flake8** — Linting
- **py_compile** — Syntax validation on all Python files

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow (Astronomer) |
| Compute | AWS EMR Serverless (PySpark) |
| Storage | AWS S3 |
| Warehouse | Snowflake |
| Transformation | dbt (orchestrated) |
| CI/CD | GitHub Actions |
| Language | Python 3.12 |

## License

MIT
