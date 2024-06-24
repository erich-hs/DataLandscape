import os
import json
import time
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from .aws_s3 import upload_to_s3
from .log_config import setup_s3_logging


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
            fetched_logs.extend(
                [event["message"] for event in response["events"]])
            # if the response is empty there is no nextToken in it
            next_token = response.get("nextToken") or next_token
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # we land here when the log groups/streams don't exist yet
            print(
                "No new Glue driver logs so far.\n"
                "If this persists, check the CloudWatch dashboard at: %r.",
                f"https://us-west-2.console.aws.amazon.com/cloudwatch/home",
            )
        else:
            raise

    if len(fetched_logs):
        # Add a tab to indent those logs and distinguish them from airflow logs.
        # Log lines returned already contain a newline character at the end.
        messages = "\t".join(fetched_logs)
        print("Glue Job Run %s Logs:\n\t%s", log_group, messages)
    else:
        print("No new log from the Glue Job in %s", log_group)
    return next_token


def check_job_status(glue_client, job_name, job_run_id):
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    status = response['JobRun']
    return status


def submit_glue_job(
    job_name: str,
    job_description: str,
    run_arguments: dict,
    script_path: str,
    s3_bucket: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
    additional_python_modules: list,
    local_log_dir: Optional[str] = None
):
    s3_client = boto3.client("s3")

    logs_client = boto3.client("logs",
                               region_name=aws_region,
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

    glue_client = boto3.client("glue",
                               region_name=aws_region,
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

    logger = setup_s3_logging(
        logger_name='glue',
        s3_bucket=s3_bucket,
        s3_log_dir='logs',
        s3_client=s3_client,
        local_log_dir=local_log_dir
    )

    s3_script_path = upload_to_s3(
        local_file=script_path,
        s3_bucket=s3_bucket,
        s3_file='scripts/' + os.path.basename(script_path),
        s3_client=s3_client,
        logger=logger
    )

    spark_configurations = [
        'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.glue_catalog.warehouse=s3://mad-dashboard-s3-001/data/mad_dashboard_dl',
        'spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
        'spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO'
    ]

    spark_string = ' --conf '.join(spark_configurations)

    job_args = {
        'Description': job_description,
        'Role': 'glue-full-access',
        'ExecutionProperty': {
            "MaxConcurrentRuns": 3
        },
        'Command': {
            "Name": "glueetl",
            "ScriptLocation": s3_script_path,
            "PythonVersion": "3"
        },
        'DefaultArguments': {
            '--conf': spark_string,
            '--datalake-formats': 'iceberg',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': 's3://mad-dashboard-s3-001/logs/spark/'
        },
        'GlueVersion': '4.0',
        'WorkerType': 'Standard',
        'NumberOfWorkers': 1
    }

    if additional_python_modules:
        job_args['DefaultArguments']['--additional-python-modules'] = ','.join(additional_python_modules)

    error_continuation_token = None
    output_continuation_token = None

    logger.info(f"Creating Glue job '{job_name}' with arguments: {json.dumps(job_args, indent=4)}")

    try:
        # Try to get the existing job
        glue_client.get_job(JobName=job_name)
        logger.info(f"Job '{job_name}' already exists. Updating it.")

        # Update the existing job
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
        logger.info(f"Job update response:\n{json.dumps(response, indent=4)}")

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Job '{job_name}' does not exist. Creating a new job.")
            response = glue_client.create_job(Name=job_name, **job_args)
            logger.info(f"Glue Client create job response:\n{json.dumps(response, indent=4)}")
        else:
            logger.error(f"Unexpected error: {e}")

    run_response = glue_client.start_job_run(JobName=job_name, Arguments=run_arguments)
    
    start_time = time.time()

    log_group_default = "/aws-glue/jobs/output"
    log_group_error = "/aws-glue/jobs/error"
    job_run_id = run_response['JobRunId']
    paginator = logs_client.get_paginator("filter_log_events")
    while True:
        status = check_job_status(glue_client, job_name, job_run_id)
        logger.info(f"Job status: {status['JobRunState']}")

        if status['JobRunState'] in ['SUCCEEDED']:
            end_time = time.time()
            logger.info(f"Job has succeeded! Time elapsed: {end_time - start_time:.2f} seconds")
            break
        elif status['JobRunState'] in ['FAILED', 'STOPPED']:
            end_time = time.time()
            logger.error(f"Job has failed or stopped! Time elapsed: {end_time - start_time:.2f} seconds")
            raise ValueError('Job has failed or stopped!')

        output_continuation_token = display_logs_from(
            paginator,
            job_run_id,
            log_group_default,
            output_continuation_token
        )
        error_continuation_token = display_logs_from(
            paginator,
            job_run_id,
            log_group_error,
            error_continuation_token
        )
        time.sleep(10)
    return job_name