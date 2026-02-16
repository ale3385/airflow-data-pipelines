import logging
import subprocess

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project/collect"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt_project/collect/profiles"


def run_dbt_command(command, select=None, exclude=None, full_refresh=False):
    """Execute a dbt command and return the result."""
    cmd = ["dbt", command, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR]

    if select:
        cmd.extend(["--select", select])
    if exclude:
        cmd.extend(["--exclude", exclude])
    if full_refresh and command == "run":
        cmd.append("--full-refresh")

    logger.info("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        logger.error("dbt %s failed:\n%s", command, result.stderr)
        raise RuntimeError(f"dbt {command} failed: {result.stderr}")

    logger.info("dbt %s completed successfully", command)
    return result.stdout
