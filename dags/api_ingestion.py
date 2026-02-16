"""Generic API ingestion pipeline: pull data, stage to S3, load to Snowflake."""

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from utils.aws_helpers import S3_BUCKET, upload_file_to_s3
from utils.snowflake_helpers import copy_into_snowflake

logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRONMENT", "DEV").upper()
API_BASE_URL = "https://api.example.com/v1"
API_KEY = os.getenv("VENDOR_API_KEY", "")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="api_vendor_ingestion",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "api", "vendor"],
) as dag:

    @task
    def extract_from_api(**context):
        ds = context["ds"]
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        endpoint = f"{API_BASE_URL}/claims"
        params = {"date": ds, "page_size": 1000}

        all_records = []
        page = 1

        while True:
            params["page"] = page
            response = requests.get(
                endpoint, headers=headers, params=params, timeout=60
            )
            response.raise_for_status()
            data = response.json()
            records = data.get("results", [])

            if not records:
                break

            all_records.extend(records)
            page += 1

            if not data.get("has_next"):
                break

        if not all_records:
            raise AirflowSkipException(f"No records from API for {ds}")

        logger.info("Extracted %d records for %s", len(all_records), ds)
        return {"record_count": len(all_records), "date": ds, "records": all_records}

    @task
    def stage_to_s3(extract_result, **context):
        ds = context["ds"]
        records = extract_result["records"]
        s3_key = f"landing/vendor_claims/{ds}/data.json"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
            tmp_path = f.name

        upload_file_to_s3(tmp_path, S3_BUCKET, s3_key)
        os.unlink(tmp_path)

        return {"s3_key": s3_key, "record_count": extract_result["record_count"]}

    @task
    def load_to_snowflake(stage_result, **context):
        ds = context["ds"]
        copy_into_snowflake(
            schema="raw",
            table="vendor_claims_raw",
            s3_path=f"landing/vendor_claims/{ds}/",
            file_format="json_format",
        )
        return {"rows_staged": stage_result["record_count"]}

    extracted = extract_from_api()
    staged = stage_to_s3(extracted)
    loaded = load_to_snowflake(staged)
