import json
from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job
from include.schemas.reddit import reddit_projects_mentions_create_table_query
from include.reference import TRACKED_PROJECTS_JSON

START_DATE = datetime(2025, 1, 5)

REDDIT_PROJECTS_MENTIONS_TABLE = 'reddit_projects_mentions'
SUBMISSIONS_TABLE = 'reddit_submissions'
COMMENTS_TABLE = 'reddit_comments'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

LLM_MODEL = 'gpt-4o-mini-2024-07-18'
LLM_MAX_COMPLETION_TOKENS = 1000
LLM_TEMPERATURE = 0.5
LLM_TIMEOUT = 60


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
    catchup=True,
    tags=["glue", "reddit", "process"]
)
def process_reddit_dag():
    process_reddit_mentions_script_path = "include/scripts/spark/spark_job_reddit_mentions.py"

    # OpenAI, guidance, and TextBlob dependencies
    additional_python_modules=[
        'pydantic==2.9.2',
        'pydantic_core==2.23.4',
        'openai==1.55.3'
    ]

    local_logs_dir = None

    wait_for_reddit_table = ExternalTaskSensor(
        task_id="wait_for_reddit_table",
        external_dag_id="load_reddit",
        external_task_id="ingest_to_production_tables"
    )

    create_reddit_projects_mentions_table = AthenaOperator(
        task_id="create_reddit_projects_mentions_table",
        depends_on_past=False,
        query=reddit_projects_mentions_create_table_query(
            target_table=REDDIT_PROJECTS_MENTIONS_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{REDDIT_PROJECTS_MENTIONS_TABLE}'
        ),
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
                    "--tracked_projects_json": json.dumps(TRACKED_PROJECTS_JSON),
                    "--submissions_table": SUBMISSIONS_TABLE,
                    "--comments_table": COMMENTS_TABLE,
                    "--target_table": REDDIT_PROJECTS_MENTIONS_TABLE,
                    "--llm_model": LLM_MODEL,
                    "--llm_max_completion_tokens": str(LLM_MAX_COMPLETION_TOKENS),
                    "--llm_temperature": str(LLM_TEMPERATURE),
                    "--llm_timeout": str(LLM_TIMEOUT)
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

    optimize_and_vacuum_reddit_mentions_table = AthenaOperator(
        task_id="optimize_and_vacuum_reddit_mentions_table",
        depends_on_past=False,
        query=f"OPTIMIZE {REDDIT_PROJECTS_MENTIONS_TABLE} REWRITE DATA USING BIN_PACK; VACUUM {REDDIT_PROJECTS_MENTIONS_TABLE};",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    (
        [
            wait_for_reddit_table,
            create_reddit_projects_mentions_table
        ] >> 
        process_reddit_mentions >>
        optimize_and_vacuum_reddit_mentions_table
    )

process_reddit_dag()