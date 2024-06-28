from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from airflow.operators.latest_only_operator import LatestOnlyOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job

START_DATE = datetime(2024, 6, 27)

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

@dag(
    "reddit_transform",
    description="Preprocess raw Reddit Submissions and Comments data.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=False,
    tags=["glue", "reddit", "transform"]
)
def reddit_transform_dag():
    transform_reddit_script_path = "include/scripts/spark/spark_job_transform_reddit.py"
    source_dir = "data/reddit/api"

    local_logs_dir = None

    transform_to_staging_table = PythonOperator(
        task_id="ingest_to_staging_table",
        depends_on_past=False,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'transform_reddit_to_staging',
            "job_description": 'Transform raw Reddit Submissions and Comments data and load to staging table.',
            "run_arguments": {
                "--ds": "{{ ds }}",
                "--s3_bucket": S3_BUCKET,
                "--source_dir": f"{source_dir}/" + "{{ ds }}" + "/valid"
            },
            "script_path": transform_reddit_script_path,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_DEFAULT_REGION,
            # "additional_python_modules": additional_python_modules,
            "local_logs_dir": local_logs_dir,
            # "spark_configs": spark_configs
        }
    )

    transform_to_staging_table

reddit_transform_dag()
