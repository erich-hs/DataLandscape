from airflow.decorators import task
from airflow.models import DagRun


def _get_ds(
    dag_run: DagRun | None = None, triggering_asset_events: dict | None = None
) -> str:
    """Determine the data partition date (ds) from dag_run or triggering_asset_events."""
    ds = None
    if triggering_asset_events:
        event = next(iter(triggering_asset_events.values()))[0]
        ds = event.timestamp.strftime("%Y-%m-%d")
    elif dag_run and dag_run.data_interval_start:
        ds = dag_run.data_interval_start.strftime("%Y-%m-%d")

    if not ds:
        raise ValueError("Could not determine data partition date (ds).")
    return ds


def glue_job_task_factory(
    *,
    task_id: str,
    job_name: str,
    job_description: str,
    script_path: str,
    run_arguments: dict,
    aws_region: str | None = None,
    s3_bucket: str | None = None,
    job_role: str | None = None,
    spark_configurations: dict | None = None,
    additional_default_job_arguments: dict | None = None,
    additional_python_modules: list[str] | None = None,
):
    """
    A factory for creating Airflow tasks that submit an AWS Glue job.

    This factory abstracts the boilerplate code for:
    - Initializing AWS hooks (Glue, S3, CloudWatch Logs).
    - Determining the execution date string 'ds'.
    - Calling a generic 'submit_glue_job' function.
    - Fetching default configurations from Airflow Variables if not provided.
    """

    @task(task_id=task_id)
    def glue_job_task(
        dag_run: DagRun | None = None, triggering_asset_events: dict | None = None
    ):
        """Airflow task to submit a Glue job."""
        from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
        from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from include.utils.aws_glue import submit_glue_job

        _aws_region = aws_region
        if _aws_region is None:
            # _aws_region = Variable.get("aws_region")
            _aws_region = "us-west-2"

        _s3_bucket = s3_bucket
        if _s3_bucket is None:
            # _s3_bucket = Variable.get("artifacts_s3_bucket")
            _s3_bucket = "dl-dev-s3bucket-aux01"

        _job_role = job_role
        if _job_role is None:
            # _job_role = Variable.get("glue_job_role")
            _job_role = "dl-dev-role-glue01"

        _spark_configurations = spark_configurations
        if _spark_configurations is None:
            # _spark_configurations = Variable.get("spark_configurations", deserialize_json=True)
            _spark_configurations = {
                "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-west-2:533267070818:bucket/dl-dev-s3tablebucket-raw01",
                "spark.jars.packages": "io.openlineage:openlineage-spark_2.12:1.26.0",
                "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                "spark.openlineage.transport.type": "http",
                "spark.openlineage.transport.url": "https://oleander.dev",
                "spark.openlineage.transport.auth.type": "api_key",
                "spark.openlineage.transport.auth.apiKey": "***",
                "spark.openlineage.namespace": "datalandscape",
            }

        _additional_default_job_arguments = additional_default_job_arguments
        if _additional_default_job_arguments is None:
            # _additional_default_job_arguments = Variable.get("additional_default_job_arguments", deserialize_json=True)
            _additional_default_job_arguments = {
                "--spark-event-logs-path": f"s3://{_s3_bucket}/logs/spark/",
                "--extra-jars": f"s3://{_s3_bucket}/jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar",
            }

        aws_conn_id = "aws_default"
        glue_hook = GlueJobHook(aws_conn_id=aws_conn_id, region_name=_aws_region)
        glue_client = glue_hook.get_conn()
        s3_hook = S3Hook(aws_conn_id=aws_conn_id, region_name=_aws_region)
        s3_client = s3_hook.get_conn()
        logs_hook = AwsLogsHook(aws_conn_id=aws_conn_id, region_name=_aws_region)
        logs_client = logs_hook.get_conn()

        ds = _get_ds(dag_run, triggering_asset_events)

        final_run_arguments = run_arguments.copy()
        final_run_arguments["--ds"] = ds

        submit_glue_job(
            job_name=job_name,
            job_description=job_description,
            run_arguments=final_run_arguments,
            script_path=script_path,
            s3_bucket=_s3_bucket,
            glue_client=glue_client,
            s3_client=s3_client,
            logs_client=logs_client,
            job_role=_job_role,
            spark_configurations=_spark_configurations,
            additional_default_job_arguments=_additional_default_job_arguments,
            additional_python_modules=additional_python_modules or [],
        )

    return glue_job_task
