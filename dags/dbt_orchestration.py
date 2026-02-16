"""dbt orchestration DAG with staged execution and testing."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from utils.dbt_helpers import run_dbt_command

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_daily_run",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "transformation"],
) as dag:

    @task
    def dbt_deps():
        return run_dbt_command("deps")

    @task
    def dbt_seed():
        return run_dbt_command("seed")

    @task
    def dbt_snapshot():
        return run_dbt_command("snapshot")

    @task
    def dbt_run_staging():
        return run_dbt_command("run", select="staging")

    @task
    def dbt_test_staging():
        return run_dbt_command("test", select="staging")

    @task
    def dbt_run_marts():
        return run_dbt_command("run", select="marts")

    @task
    def dbt_test_marts():
        return run_dbt_command("test", select="marts")

    @task
    def dbt_freshness():
        return run_dbt_command("source", select="freshness")

    deps = dbt_deps()
    seed = dbt_seed()
    snapshot = dbt_snapshot()
    run_stg = dbt_run_staging()
    test_stg = dbt_test_staging()
    run_marts = dbt_run_marts()
    test_marts = dbt_test_marts()
    freshness = dbt_freshness()

    deps >> seed >> snapshot >> run_stg >> test_stg >> run_marts >> test_marts
    deps >> freshness
