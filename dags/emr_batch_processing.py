"""EMR Serverless batch processing pipeline for claims data."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from utils.aws_helpers import (
    S3_BUCKET,
    delete_s3_prefix,
    get_aws_session,
    list_objects_paginated,
    upload_file_to_s3,
)
from utils.emr_helpers import (
    create_emr_application,
    delete_emr_application,
    start_emr_job,
    wait_for_job_completion,
)
from utils.snowflake_helpers import copy_into_snowflake

ENV = os.getenv("ENVIRONMENT", "DEV").upper()
VENDOR = "claims"
SCRIPT_LOCAL = "/usr/local/airflow/spark_jobs/process_claims.py"
SCRIPT_S3 = f"emr/{VENDOR}/scripts/process_claims.py"
OUTPUT_PREFIX = f"emr/{VENDOR}/output"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="claims_emr_batch_processing",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["claims", "emr", "batch"],
) as dag:

    @task
    def discover_files(**context):
        ds = context["ds"]
        prefix = f"raw/{VENDOR}/{ds}/"
        files = list_objects_paginated(S3_BUCKET, prefix, suffix=".json")
        if not files:
            raise AirflowSkipException(f"No files found for {ds}")
        return {"files": files, "count": len(files)}

    @task
    def upload_spark_script():
        upload_file_to_s3(SCRIPT_LOCAL, S3_BUCKET, SCRIPT_S3)
        return f"s3://{S3_BUCKET}/{SCRIPT_S3}"

    @task
    def create_application():
        session = get_aws_session()
        emr = session.client("emr-serverless")
        app_id = create_emr_application(emr, f"{VENDOR}-processing-{ENV.lower()}")
        return app_id

    @task
    def run_spark_job(app_id, script_uri, file_info, **context):
        ds = context["ds"]
        session = get_aws_session()
        emr = session.client("emr-serverless")

        arguments = [
            "--input-prefix", f"s3://{S3_BUCKET}/raw/{VENDOR}/{ds}/",
            "--output-prefix", f"s3://{S3_BUCKET}/{OUTPUT_PREFIX}/{ds}/",
            "--file-count", str(file_info["count"]),
        ]

        job_id = start_emr_job(emr, app_id, script_uri, arguments, VENDOR)
        wait_for_job_completion(emr, app_id, job_id)
        return job_id

    @task
    def load_to_snowflake(**context):
        ds = context["ds"]
        copy_into_snowflake(
            schema="raw",
            table="claims_raw",
            s3_path=f"{OUTPUT_PREFIX}/{ds}/",
            file_format="parquet_format",
        )

    @task
    def cleanup(app_id, **context):
        ds = context["ds"]
        delete_s3_prefix(S3_BUCKET, f"{OUTPUT_PREFIX}/{ds}/")
        session = get_aws_session()
        emr = session.client("emr-serverless")
        delete_emr_application(emr, app_id)

    file_info = discover_files()
    script_uri = upload_spark_script()
    app_id = create_application()
    job_result = run_spark_job(app_id, script_uri, file_info)
    load = load_to_snowflake()
    clean = cleanup(app_id)

    job_result >> load >> clean
