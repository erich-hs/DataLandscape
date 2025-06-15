from pendulum import datetime, duration
from airflow.sdk import Asset
from airflow.decorators import dag, task

reddit_raw_files_s3 = Asset(
    "reddit_raw_files_s3",
    uri="s3://dl-dev-s3bucket-landingzone01/data/reddit/api",
    group="reddit",
)
reddit_comments_raw = Asset("raw.reddit.comments", group="reddit")
reddit_submissions_raw = Asset("raw.reddit.submissions", group="reddit")


@dag(
    schedule=[reddit_raw_files_s3],
    start_date=datetime(2025, 6, 6),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=duration(minutes=30),
    default_args={"retries": 0},
    description="Loads Reddit data from S3 to raw Iceberg tables.",
    tags=["reddit", "load", "raw"],
)
def load_reddit_to_raw_dag():
    from airflow.providers.amazon.aws.hooks.glue import GlueJobHook  # type: ignore
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
    from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook  # type: ignore
    from include.utils.aws_glue import submit_glue_job

    TARGET_SUBMISSIONS_TABLE = "reddit__submissions"
    TARGET_COMMENTS_TABLE = "reddit__comments"
    TARGET_DATABASE = "raw"
    SOURCE_S3_BUCKET = "dl-dev-s3bucket-landingzone01"
    SOURCE_DIR = "data/reddit/api"
    LOAD_REDDIT_SCRIPT_PATH = "include/spark/spark_job_load_reddit_from_s3.py"
    ARTIFACTS_S3_BUCKET = "dl-dev-s3bucket-aux01"
    AWS_REGION = "us-west-2"
    GLUE_JOB_ROLE = "dl-dev-role-glue01"
    SPARK_CONFIGURATIONS = {
        "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-west-2:533267070818:bucket/dl-dev-s3tablebucket-raw01"
    }
    ADDITIONAL_DEFAULT_JOB_ARGUMENTS = {
        "--spark-event-logs-path": f"s3://{ARTIFACTS_S3_BUCKET}/logs/spark/",
        "--extra-jars": f"s3://{ARTIFACTS_S3_BUCKET}/jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar",
    }

    @task(outlets=[reddit_comments_raw, reddit_submissions_raw])
    def load_reddit_to_raw_tables(dag_run=None, triggering_asset_events=None):
        glue_hook = GlueJobHook(aws_conn_id="aws_default", region_name=AWS_REGION)
        glue_client = glue_hook.get_conn()
        s3_hook = S3Hook(aws_conn_id="aws_default", region_name=AWS_REGION)
        s3_client = s3_hook.get_conn()
        logs_hook = AwsLogsHook(aws_conn_id="aws_default", region_name=AWS_REGION)
        logs_client = logs_hook.get_conn()

        ds = None
        if triggering_asset_events:
            # A single asset event is expected for this dag
            event = next(iter(triggering_asset_events.values()))[0]
            ds = event.timestamp.strftime("%Y-%m-%d")
        elif dag_run and dag_run.data_interval_start:
            ds = dag_run.data_interval_start.strftime("%Y-%m-%d")

        if not ds:
            raise ValueError("Could not determine data partition date (ds).")

        submit_glue_job(
            job_name="load_reddit_to_raw_tables",
            job_description="Load Reddit Submissions and Comments data from S3 to raw Iceberg tables.",
            run_arguments={
                "--ds": ds,
                "--target_submissions_table": TARGET_SUBMISSIONS_TABLE,
                "--target_comments_table": TARGET_COMMENTS_TABLE,
                "--target_database": TARGET_DATABASE,
                "--source_s3_bucket": SOURCE_S3_BUCKET,
                "--source_dir": f"{SOURCE_DIR}/{ds}/valid",
            },
            script_path=LOAD_REDDIT_SCRIPT_PATH,
            s3_bucket=ARTIFACTS_S3_BUCKET,
            glue_client=glue_client,
            s3_client=s3_client,
            logs_client=logs_client,
            job_role=GLUE_JOB_ROLE,
            spark_configurations=SPARK_CONFIGURATIONS,
            additional_default_job_arguments=ADDITIONAL_DEFAULT_JOB_ARGUMENTS,
        )

    load_reddit_to_raw_tables()


load_reddit_to_raw_dag()
