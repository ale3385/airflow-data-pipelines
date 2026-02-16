import logging
import os
import time

logger = logging.getLogger(__name__)

EMR_EXECUTION_ROLE = os.getenv("EMR_EXECUTION_ROLE_ARN")


def get_emr_config(vendor, memory="4G", cores="2"):
    """Return standard EMR Serverless Spark configuration."""
    return {
        "sparkSubmitParameters": (
            f"--conf spark.executor.memory={memory} "
            f"--conf spark.executor.cores={cores} "
            f"--conf spark.dynamicAllocation.enabled=true "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
        ),
    }


def create_emr_application(emr_client, app_name, release_label="emr-7.1.0"):
    """Create an EMR Serverless application and return the application ID."""
    response = emr_client.create_application(
        name=app_name,
        releaseLabel=release_label,
        type="SPARK",
        autoStartConfiguration={"enabled": True},
        autoStopConfiguration={"enabled": True, "idleTimeoutMinutes": 5},
    )
    app_id = response["applicationId"]
    logger.info("Created EMR application %s (%s)", app_name, app_id)

    waiter_start = time.time()
    while True:
        status = emr_client.get_application(applicationId=app_id)
        state = status["application"]["state"]
        if state == "CREATED":
            break
        if time.time() - waiter_start > 300:
            raise TimeoutError(f"EMR app {app_id} stuck in {state}")
        time.sleep(10)

    return app_id


def start_emr_job(emr_client, app_id, script_s3_uri, arguments, vendor):
    """Submit a Spark job to EMR Serverless."""
    bucket = os.getenv("S3_BUCKET")
    config = get_emr_config(vendor)

    response = emr_client.start_job_run(
        applicationId=app_id,
        executionRoleArn=EMR_EXECUTION_ROLE,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_s3_uri,
                "entryPointArguments": arguments,
                **config,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{bucket}/emr/{vendor}/logs/"
                }
            }
        },
    )
    job_id = response["jobRunId"]
    logger.info("Started EMR job %s on app %s", job_id, app_id)
    return job_id


def wait_for_job_completion(emr_client, app_id, job_id, timeout=3600):
    """Poll EMR job until terminal state. Raises on failure."""
    start = time.time()
    while True:
        response = emr_client.get_job_run(applicationId=app_id, jobRunId=job_id)
        state = response["jobRun"]["state"]

        if state == "SUCCESS":
            logger.info("EMR job %s completed successfully", job_id)
            return state
        if state in ("FAILED", "CANCELLED"):
            msg = response["jobRun"].get("stateDetails", "No details")
            raise RuntimeError(f"EMR job {job_id} {state}: {msg}")
        if time.time() - start > timeout:
            raise TimeoutError(f"EMR job {job_id} timed out after {timeout}s")

        time.sleep(30)


def delete_emr_application(emr_client, app_id):
    """Delete an EMR Serverless application."""
    emr_client.delete_application(applicationId=app_id)
    logger.info("Deleted EMR application %s", app_id)
