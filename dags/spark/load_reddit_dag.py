import logging
from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from datetime import datetime, timedelta
from include.schemas.reddit import reddit_submissions_create_table_query, reddit_comments_create_table_query
from include.utils.aws_glue import submit_glue_job

START_DATE = datetime(2025, 1, 6)

SUBMISSIONS_TABLE = 'reddit_submissions'
COMMENTS_TABLE = 'reddit_comments'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

REDDIT_API_SOURCE_DIR = "data/reddit/api"
REDDIT_API_ARCHIVE_DIR = "data/reddit/api/archive"


def archive_s3_objects(
    source_bucket_name: str,
    source_bucket_dir: str,
    dest_bucket_name: str,
    dest_bucket_dir: str,
):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    keys = s3_hook.list_keys(bucket_name=source_bucket_name, prefix=source_bucket_dir)
    logging.info(f"Archiving {len(keys)} objects from {source_bucket_dir}/ to {dest_bucket_dir}/")
    for key in keys:
        source_bucket_key = key
        dest_bucket_key = key.replace(source_bucket_dir, dest_bucket_dir)

        # Archive
        s3_hook.copy_object(
            source_bucket_name=source_bucket_name,
            source_bucket_key=source_bucket_key,
            dest_bucket_name=dest_bucket_name,
            dest_bucket_key=dest_bucket_key,
            acl_policy='bucket-owner-full-control'
        )
        logging.info(f"Archived {source_bucket_key} to {dest_bucket_key}")

        # Cleanup
        s3_hook.delete_objects(
            bucket=source_bucket_name,
            keys=[source_bucket_key]
        )
        logging.info(f"Deleted {source_bucket_key}")

@dag(
    "load_reddit",
    description="Write Reddit Submissions and Comments to Iceberg tables from raw source.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=True,
    tags=["glue", "reddit", "wap"]
)
def load_reddit_dag():
    ingest_reddit_script_path = "include/scripts/spark/spark_job_ingest_reddit.py"

    local_logs_dir = None

    wait_for_reddit_data = ExternalTaskSensor(
        task_id="wait_for_reddit_data",
        external_dag_id="ingest_reddit",
        external_task_id="fetch_reddit"
    )

    create_submissions_table = AthenaOperator(
        task_id="create_submissions_table",
        depends_on_past=False,
        query=reddit_submissions_create_table_query(target_table=SUBMISSIONS_TABLE, location=f'{S3_BUCKET}/data/mad_dashboard_dl/{SUBMISSIONS_TABLE}'),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    create_comments_table = AthenaOperator(
        task_id="create_comments_table",
        depends_on_past=False,
        query=reddit_comments_create_table_query(target_table=COMMENTS_TABLE, location=f'{S3_BUCKET}/data/mad_dashboard_dl/{COMMENTS_TABLE}'),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    ingest_to_production_tables = PythonOperator(
        task_id="ingest_to_production_tables",
        depends_on_past=True,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'ingest_reddit_to_production',
            "job_description": 'Ingest Reddit Submissions and Comments data to production Iceberg tables.',
            "run_arguments": {
                "--ds": "{{ ds }}",
                "--target_submissions_table": SUBMISSIONS_TABLE,
                "--target_comments_table": COMMENTS_TABLE,
                "--s3_bucket": S3_BUCKET,
                "--source_dir": f"{REDDIT_API_SOURCE_DIR}/" + "{{ macros.ds_add(ds, 1) }}" + "/valid"
            },
            "script_path": ingest_reddit_script_path,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_DEFAULT_REGION,
            "local_logs_dir": local_logs_dir,
        }
    )

    archive_reddit_data = PythonOperator(
        task_id="archive_reddit_data",
        depends_on_past=True,
        python_callable=archive_s3_objects,
        op_kwargs={
            "source_bucket_name": S3_BUCKET,
            "source_bucket_dir": f"{REDDIT_API_SOURCE_DIR}/" + "{{ macros.ds_add(ds, 1) }}",
            "dest_bucket_name": S3_BUCKET,
            "dest_bucket_dir": f"{REDDIT_API_ARCHIVE_DIR}/" + "{{ macros.ds_add(ds, 1) }}"
        }
    )

    (
        wait_for_reddit_data >>
        [
            create_submissions_table,
            create_comments_table
        ] >>
        ingest_to_production_tables >>
        archive_reddit_data
    )

load_reddit_dag()
