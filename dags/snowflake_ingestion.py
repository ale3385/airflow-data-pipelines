"""S3 to Snowflake ingestion pipeline using COPY INTO."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from utils.aws_helpers import S3_BUCKET, list_objects_paginated
from utils.snowflake_helpers import (
    DATABASE,
    execute_and_fetch_snowflake_query,
    execute_snowflake_query,
)

ENV = os.getenv("ENVIRONMENT", "DEV").upper()

INGESTION_CONFIGS = [
    {
        "source": "providers",
        "schema": "raw",
        "table": "providers_raw",
        "s3_prefix": "landing/providers/",
        "file_format": "csv_format",
        "file_suffix": ".csv",
    },
    {
        "source": "eligibility",
        "schema": "raw",
        "table": "eligibility_raw",
        "s3_prefix": "landing/eligibility/",
        "file_format": "parquet_format",
        "file_suffix": ".parquet",
    },
    {
        "source": "pharmacy",
        "schema": "raw",
        "table": "pharmacy_claims_raw",
        "s3_prefix": "landing/pharmacy/",
        "file_format": "json_format",
        "file_suffix": ".json",
    },
]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="snowflake_s3_ingestion",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "snowflake", "s3"],
) as dag:

    for config in INGESTION_CONFIGS:

        @task(task_id=f"check_files_{config['source']}")
        def check_new_files(cfg=config):
            files = list_objects_paginated(
                S3_BUCKET, cfg["s3_prefix"], suffix=cfg["file_suffix"]
            )
            if not files:
                raise AirflowSkipException(f"No new files for {cfg['source']}")
            return {"source": cfg["source"], "file_count": len(files)}

        @task(task_id=f"copy_into_{config['source']}")
        def run_copy_into(file_info, cfg=config):
            query = f"""
                COPY INTO {DATABASE}.{cfg['schema']}.{cfg['table']}
                FROM @{DATABASE}.{cfg['schema']}.s3_stage/{cfg['s3_prefix']}
                FILE_FORMAT = (FORMAT_NAME = '{DATABASE}.{cfg['schema']}.{cfg['file_format']}')
                PATTERN = '.*\\{cfg['file_suffix']}'
                ON_ERROR = 'CONTINUE'
                PURGE = FALSE
            """
            execute_snowflake_query(query)

        @task(task_id=f"validate_{config['source']}")
        def validate_load(cfg=config):
            query = f"""
                SELECT COUNT(*) FROM {DATABASE}.{cfg['schema']}.{cfg['table']}
                WHERE _loaded_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
            """
            result = execute_and_fetch_snowflake_query(query)
            row_count = result[0][0]
            if row_count == 0:
                raise RuntimeError(f"No rows loaded for {cfg['source']}")
            return {"source": cfg["source"], "rows_loaded": row_count}

        files = check_new_files()
        copy = run_copy_into(files)
        validate = validate_load()

        files >> copy >> validate
