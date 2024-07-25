from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from airflow.operators.latest_only_operator import LatestOnlyOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job

START_DATE = datetime(2024, 7, 16)

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

@dag(
    "deploy_evidence_dashboard",
    description="Write source files and deploy the evidence.dev dashboard.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    tags=["evidence", "dashboard", "deploy"]
)
def deploy_evidence_dashboard_dag():
    write_evidence_objects_script_path = 'include/scripts/spark/spark_job_write_evidence_objects.py'
    local_logs_dir = None

    write_evidence_objects = PythonOperator(
        task_id="write_evidence_objects",
        depends_on_past=False,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'write_evidence_objects',
            "job_description": 'Write source Parquet files for the evidence.dev dashboard.',
            "run_arguments": {
                "--ds": "{{ ds }}"
            },
            "script_path": write_evidence_objects_script_path,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_DEFAULT_REGION,
            "local_logs_dir": local_logs_dir
        }
    )

    write_evidence_objects

deploy_evidence_dashboard_dag()