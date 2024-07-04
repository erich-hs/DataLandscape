import logging
from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job
from include.reference import PROJECTS_SEARCH_TERMS

START_DATE = datetime(2024, 7, 21)

REDDIT_PROJECTS_MENTIONS_TABLE = 'reddit_projects_mentions'
SUBMISSIONS_TABLE = 'reddit_submissions'
COMMENTS_TABLE = 'reddit_comments'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

LLM_TEMPERATURE = 0.5

def reddit_projects_mentions_create_table_query(target_table):
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
        content_type STRING,
        created_utc DOUBLE,
        created_date DATE,
        title STRING,
        id STRING,
        subreddit STRING,
        text STRING,
        permalink STRING,
        score INT,
        projects_mentions ARRAY<STRUCT<project:STRING, mentions:INT>>,
        projects_mentions_polarity ARRAY<STRUCT<project:STRING, summary:STRING, polarity:DOUBLE>>
    )
    PARTITIONED BY (created_date)
    LOCATION 's3://{S3_BUCKET}/data/mad_dashboard_dl/{target_table}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy'
    )
    """

@dag(
    "process_reddit",
    description="Process Reddit Submissions and Comments mentions of tracked projects.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=False,
    tags=["glue", "reddit", "process"]
)
def process_reddit_dag():
    process_reddit_mentions_script_path = "include/scripts/spark/spark_job_reddit_mentions.py"

    # OpenAI, guidance, and TextBlob dependencies
    additional_python_modules=[
        'guidance==0.1.15',
        'openai==1.35.7',
        'textblob==0.18.0.post0'
    ]

    local_logs_dir = None

    wait_for_reddit_table = ExternalTaskSensor(
        task_id="wait_for_reddit_table",
        external_dag_id="load_reddit",
        external_task_id="ingest_to_production_tables"
    )

    create_reddit_projects_mentions_table_if_not_exists = AthenaOperator(
        task_id="create_submissions_table_if_not_exists",
        depends_on_past=False,
        query=reddit_projects_mentions_create_table_query(REDDIT_PROJECTS_MENTIONS_TABLE),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    process_reddit_mentions = PythonOperator(
            task_id="process_reddit_mentions",
            depends_on_past=False,
            python_callable=submit_glue_job,
            op_kwargs={
                "job_name": 'process_reddit_mentions',
                "job_description": 'Process Reddit Submissions and Comments mentions of tracked projects.',
                "run_arguments": {
                    "--ds": "{{ ds }}",
                    "--tracked_projects": ",".join(PROJECTS_SEARCH_TERMS),
                    "--submissions_table": SUBMISSIONS_TABLE,
                    "--comments_table": COMMENTS_TABLE,
                    "--target_table": REDDIT_PROJECTS_MENTIONS_TABLE,
                    "--llm_temperature": str(LLM_TEMPERATURE)
                },
                "script_path": process_reddit_mentions_script_path,
                "s3_bucket": S3_BUCKET,
                "aws_access_key_id": AWS_ACCES_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                "aws_region": AWS_DEFAULT_REGION,
                "additional_python_modules": additional_python_modules,
                "local_logs_dir": local_logs_dir,
            }
        )
    
    (
        [
            wait_for_reddit_table,
            create_reddit_projects_mentions_table_if_not_exists
        ] >> 
        process_reddit_mentions
    )

process_reddit_dag()