import os
import json
import time
from typing import List, Optional, Dict, Any
import boto3  # type: ignore
from botocore.exceptions import ClientError, NoCredentialsError  # type: ignore
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# --- Default Configurations ---
DEFAULT_GLUE_VERSION = "5.0"
DEFAULT_WORKER_TYPE = "G.1X"
DEFAULT_NUMBER_OF_WORKERS = 2
DEFAULT_MAX_CONCURRENT_RUNS = 6

# Default Spark configurations (base set, can be augmented by caller)
DEFAULT_SPARK_CONFIGURATIONS = {
    "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.sql.defaultCatalog": "s3tablesbucket",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.s3tablesbucket.cache-enabled": "false",
}

# Default arguments for the Glue job (can be augmented by caller)
DEFAULT_JOB_ARGUMENTS = {"--datalake-formats": "iceberg", "--enable-spark-ui": "true"}
# --- End Default Configurations ---


def _build_glue_job_args(
    job_description: str,
    s3_script_path: str,
    spark_string: str,
    role: str,
    glue_version: str,
    worker_type: str,
    number_of_workers: int,
    max_concurrent_runs: int,
    default_arguments: Dict[str, str],  # Expects the final default_arguments dict
    additional_python_modules: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Builds the arguments dictionary for creating or updating a Glue job."""
    # Prepare the DefaultArguments dictionary first to resolve type checker issues
    final_default_arguments = default_arguments.copy()
    final_default_arguments["--conf"] = spark_string
    if additional_python_modules:
        final_default_arguments["--additional-python-modules"] = ",".join(
            additional_python_modules
        )

    job_args: Dict[str, Any] = {
        "Description": job_description,
        "Role": role,
        "ExecutionProperty": {"MaxConcurrentRuns": max_concurrent_runs},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": s3_script_path,
            "PythonVersion": "3",
        },
        "DefaultArguments": final_default_arguments,
        "GlueVersion": glue_version,
        "WorkerType": worker_type,
        "NumberOfWorkers": number_of_workers,
    }

    return job_args


def _create_or_update_glue_job(glue_client, job_name: str, job_args: Dict):
    """Creates a new Glue job or updates an existing one."""
    try:
        glue_client.get_job(JobName=job_name)
        logger.info(f"Job '{job_name}' already exists. Updating it.")
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
        logger.info(f"Job update response:\n{json.dumps(response, indent=4)}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info(f"Job '{job_name}' does not exist. Creating a new job.")
            response = glue_client.create_job(Name=job_name, **job_args)
            logger.info(
                f"Glue Client create job response:\n{json.dumps(response, indent=4)}"
            )
        else:
            logger.error(
                f"Unexpected error while creating/updating job '{job_name}': {e}"
            )
            raise


def _monitor_glue_job_run(
    glue_client,
    logs_client,
    job_name: str,
    job_run_id: str,
    poll_interval: int = 10,  # Seconds
):
    """
    Monitors the status of a Glue job run, displays logs, and waits for completion.

    Args:
        glue_client: Boto3 Glue client.
        logs_client: Boto3 CloudWatch Logs client.
        job_name: The name of the Glue job.
        job_run_id: The ID of the job run to monitor.
        poll_interval: Time in seconds to wait between status checks.

    Raises:
        ValueError: If the job fails or is stopped.
    """
    logger.info(f"Monitoring Glue job '{job_name}' with Run ID: {job_run_id}")
    start_time = time.time()

    log_group_default = "/aws-glue/jobs/output"
    log_group_error = "/aws-glue/jobs/error"
    paginator = logs_client.get_paginator("filter_log_events")
    error_continuation_token = None
    output_continuation_token = None

    while True:
        status = check_job_status(glue_client, job_name, job_run_id)
        job_run_state = status.get("JobRunState")

        if job_run_state == "SUCCEEDED":
            end_time = time.time()
            logger.info(
                f"Job '{job_name}' (Run ID: {job_run_id}) succeeded! "
                f"Time elapsed: {end_time - start_time:.2f} seconds"
            )
            break
        elif job_run_state in ["FAILED", "STOPPED", "TIMEOUT"]:  # Added TIMEOUT
            end_time = time.time()
            error_message = status.get("ErrorMessage", "No error message provided.")
            logger.error(
                f"Job '{job_name}' (Run ID: {job_run_id}) ended with status: {job_run_state}. "
                f"Error: {error_message}. Time elapsed: {end_time - start_time:.2f} seconds"
            )
            # Display final logs before raising
            display_logs_from(
                paginator, job_run_id, log_group_default, output_continuation_token
            )
            display_logs_from(
                paginator, job_run_id, log_group_error, error_continuation_token
            )
            raise ValueError(
                f"Job '{job_name}' (Run ID: {job_run_id}) ended with status: {job_run_state}. "
                f"Error: {error_message}"
            )
        elif job_run_state in [
            "RUNNING",
            "STARTING",
            "STOPPING",
            "WAITING",
        ]:  # States indicating activity
            logger.info(
                f"Job '{job_name}' (Run ID: {job_run_id}) is currently: {job_run_state}"
            )
        else:  # Unknown or unexpected state
            logger.warning(
                f"Job '{job_name}' (Run ID: {job_run_id}) is in an unexpected state: {job_run_state}"
            )

        # Always try to display logs as long as the job is running or just finished
        output_continuation_token = display_logs_from(
            paginator, job_run_id, log_group_default, output_continuation_token
        )
        error_continuation_token = display_logs_from(
            paginator, job_run_id, log_group_error, error_continuation_token
        )
        time.sleep(poll_interval)


def upload_to_s3(local_file, s3_bucket, s3_file, s3_client, logger):
    try:
        # An upload_file will overwrite the file if it already exists
        s3_client.upload_file(local_file, s3_bucket, s3_file)
        logger.info(f"Upload to S3 successful: {local_file} to {s3_bucket}/{s3_file}")
        return f"s3://{s3_bucket}/{s3_file}"
    except FileNotFoundError as e:
        logger.error("The file was not found")
        raise e
    except NoCredentialsError as e:
        logger.error("Credentials not available")
        raise e
    except ClientError as e:
        logger.error(f"Client error: {e}")
        raise e


def display_logs_from(paginator, run_id, log_group: str, continuation_token):
    """Mutualize iteration over the 2 different log streams glue jobs write to."""
    fetched_logs = []
    next_token = continuation_token
    try:
        for response in paginator.paginate(
            logGroupName=log_group,
            logStreamNames=[run_id],
            PaginationConfig={"StartingToken": continuation_token},
        ):
            fetched_logs.extend([event["message"] for event in response["events"]])
            # if the response is empty there is no nextToken in it
            next_token = response.get("nextToken") or next_token
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # we land here when the log groups/streams don't exist yet
            logger.info(
                f"No new Glue logs in {log_group} yet. This is expected early in the job run."
            )
        else:
            raise

    if len(fetched_logs):
        # Add a tab to indent those logs and distinguish them from airflow logs.
        # Log lines returned already contain a newline character at the end.
        messages = "\n\t".join(log.rstrip() for log in fetched_logs)
        logger.info(f"Glue Job Run {run_id} Logs from {log_group}:\n\t{messages}")
    else:
        logger.info(f"No new log from the Glue Job in {log_group}")
    return next_token


def check_job_status(glue_client, job_name, job_run_id):
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    status = response["JobRun"]
    return status


def submit_glue_job(
    job_name: str,
    job_description: str,
    run_arguments: dict,
    script_path: str,
    s3_bucket: str,
    glue_client: boto3.client,
    s3_client: boto3.client,
    logs_client: boto3.client,
    job_role: str,
    glue_version: str = DEFAULT_GLUE_VERSION,
    worker_type: str = DEFAULT_WORKER_TYPE,
    number_of_workers: int = DEFAULT_NUMBER_OF_WORKERS,
    max_concurrent_runs: int = DEFAULT_MAX_CONCURRENT_RUNS,
    spark_configurations: Optional[Dict[str, str]] = None,
    additional_default_job_arguments: Optional[Dict[str, str]] = None,
    additional_python_modules: Optional[List[str]] = None,
    monitoring_poll_interval: int = 10,
):
    """
    Submits a job to AWS Glue, waits for it to complete, and monitors its logs.

    This function handles:
    1. Uploading the ETL script to S3.
    2. Building job arguments from defaults and custom inputs.
    3. Creating or updating the Glue job definition.
    4. Starting a new job run.
    5. Monitoring the job run's status and logs until completion.

    Args:
        job_name: The name of the Glue job.
        job_description: A description for the Glue job.
        run_arguments: Arguments to pass to the specific job run.
        script_path: Local path to the Python ETL script.
        s3_bucket: The S3 bucket for uploading scripts and for other job artifacts.
        glue_client: An initialized Boto3 Glue client.
        s3_client: An initialized Boto3 S3 client.
        logs_client: An initialized Boto3 CloudWatch Logs client.
        job_role: The IAM role for the Glue job. Defaults to a global constant.
        glue_version: The Glue version. Defaults to a global constant.
        worker_type: The worker type for the job. Defaults to a global constant.
        number_of_workers: The number of workers for the job. Defaults to a global constant.
        max_concurrent_runs: The max concurrent runs for the job definition. Defaults to a global constant.
        spark_configurations: A dictionary of Spark configurations to add to or override the defaults.
        additional_default_job_arguments: A dictionary of default arguments to add to or override the defaults.
        additional_python_modules: A list of additional Python modules for the job.
        monitoring_poll_interval: The interval in seconds for polling job status and logs.

    Returns:
        The name of the job that was run successfully.

    Raises:
        ValueError: If the job fails, is stopped, or times out.
        ClientError: If there is an issue with AWS API calls.
    """
    logger.info(f"Initiating Glue job submission for '{job_name}'")

    s3_script_path = upload_to_s3(
        local_file=script_path,
        s3_bucket=s3_bucket,
        s3_file="scripts/" + os.path.basename(script_path),
        s3_client=s3_client,
        logger=logger,
    )

    # Determine final Spark configurations
    final_spark_configs = DEFAULT_SPARK_CONFIGURATIONS.copy()
    if spark_configurations:
        final_spark_configs.update(spark_configurations)
    spark_string = " --conf ".join(
        [f"{key}={value}" for key, value in final_spark_configs.items()]
    )

    # Determine final default job arguments
    final_default_job_arguments = DEFAULT_JOB_ARGUMENTS.copy()
    if additional_default_job_arguments:
        final_default_job_arguments.update(additional_default_job_arguments)

    job_args = _build_glue_job_args(
        job_description=job_description,
        s3_script_path=s3_script_path,
        spark_string=spark_string,
        role=job_role,
        glue_version=glue_version,
        worker_type=worker_type,
        number_of_workers=number_of_workers,
        max_concurrent_runs=max_concurrent_runs,
        default_arguments=final_default_job_arguments,
        additional_python_modules=additional_python_modules,
    )

    logger.info(
        f"Creating/Updating Glue job '{job_name}' with arguments: {json.dumps(job_args, indent=4)}"
    )
    _create_or_update_glue_job(glue_client, job_name, job_args)

    logger.info(
        f"Starting Glue job run for '{job_name}' with arguments: {run_arguments}"
    )
    run_response = glue_client.start_job_run(JobName=job_name, Arguments=run_arguments)
    job_run_id = run_response["JobRunId"]

    _monitor_glue_job_run(
        glue_client=glue_client,
        logs_client=logs_client,
        job_name=job_name,
        job_run_id=job_run_id,
        poll_interval=monitoring_poll_interval,
    )

    return job_name
