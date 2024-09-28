from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from airflow.operators.latest_only_operator import LatestOnlyOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job
from include.schemas.pypi import pypi_create_table_query
from include.reference import PYPI_PROJECTS

START_DATE = datetime(2024, 7, 25)

STAGING_TABLE = 'stg_pypi_file_downloads'
PRODUCTION_TABLE = 'pypi_file_downloads'
INVALID_RECORDS_TOLERANCE = '0.01'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')


@dag(
    "load_pypi",
    description="Write, Audit, and Publish PyPI file download counts from Google BigQuery public dataset.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=True,
    tags=["glue", "pypi", "wap"]
)
def load_pypi_dag():
    ingest_pypi_script_path = 'include/scripts/spark/spark_job_ingest_pypi.py'
    audit_pypi_script_path = 'include/scripts/spark/spark_job_audit_pypi.py'

    # Google BigQuery dependencies
    additional_python_modules=[
        'google-api-core==2.19.0',
        'google-auth==2.29.0',
        'google-cloud-bigquery==3.24.0',
        'google-cloud-bigquery-storage==2.25.0',
        'google-cloud-core==2.4.1',
        'google-crc32c==1.5.0',
        'google-resumable-media==2.7.0',
        'googleapis-common-protos==1.63.1'
    ]

    spark_configs = {
        "spark.driver.memory": "16g",
    }

    local_logs_dir = None

    # Write tasks
    create_staging_table = AthenaOperator(
        task_id="create_staging_table",
        depends_on_past=False,
        query=pypi_create_table_query(target_table=STAGING_TABLE, location=f'{S3_BUCKET}/data/mad_dashboard_dl/{STAGING_TABLE}'),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    ingest_to_staging_table = PythonOperator(
        task_id="ingest_to_staging_table",
        depends_on_past=False,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'ingest_pypi_file_downloads',
            "job_description": 'Ingest aggregate PyPI file download counts from Google BigQuery public dataset.',
            "run_arguments": {
                "--ds": "{{ ds }}",
                "--pypi_project": ",".join(PYPI_PROJECTS),
                "--target_table": STAGING_TABLE
            },
            "script_path": ingest_pypi_script_path,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_DEFAULT_REGION,
            "additional_python_modules": additional_python_modules,
            "local_logs_dir": local_logs_dir,
            "spark_configs": spark_configs
        }
    )

    # LatestOnlyOperator will skip the audit and publish tasks if the current DAG run is not the latest
    # This will ensure that the audit and publish tasks are only executed once during backfills
    is_latest_dag_run = LatestOnlyOperator(task_id="is_latest_dag_run")

    # Audit task
    audit_staging_table = PythonOperator(
        task_id="audit_staging_table",
        depends_on_past=False,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'audit_pypi_file_downloads',
            "job_description": 'Audit target PyPI file download counts table for invalid records.',
            "run_arguments": {
                "--target_table": STAGING_TABLE,
                "--invalid_records_tolerance": INVALID_RECORDS_TOLERANCE
            },
            "script_path": audit_pypi_script_path,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_DEFAULT_REGION,
            "local_logs_dir": local_logs_dir
        }
    )

    # Publish tasks
    create_production_table = AthenaOperator(
        task_id="create_production_table",
        depends_on_past=False,
        query=pypi_create_table_query(target_table=PRODUCTION_TABLE, location=f'{S3_BUCKET}/data/mad_dashboard_dl/{PRODUCTION_TABLE}'),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    exchange_step = AthenaOperator(
        task_id="exchange_step",
        depends_on_past=False,
        query=f"""MERGE INTO {PRODUCTION_TABLE} t
        USING {STAGING_TABLE} s
        ON (
            s.download_date = t.download_date
            AND s.project = t.project
            AND s.project_version = t.project_version
            AND COALESCE(s.python, '') = COALESCE(t.python, '')
            AND COALESCE(s.system_name, '') = COALESCE(t.system_name, '')
            AND COALESCE(s.country_code, '') = COALESCE(t.country_code, '')
        )
        WHEN MATCHED
            THEN UPDATE SET
                download_date = s.download_date,
                project = s.project,
                project_version = s.project_version,
                python = s.python,
                system_name = s.system_name,
                country_code = s.country_code,
                download_count = s.download_count
        WHEN NOT MATCHED
            THEN INSERT (
                download_date,
                project,
                project_version,
                python,
                system_name,
                country_code,
                download_count
            )
            VALUES (
                s.download_date,
                s.project,
                s.project_version,
                s.python,
                s.system_name,
                s.country_code,
                s.download_count
            )
        """,
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    cleanup_step = AthenaOperator(
        task_id="cleanup_step",
        depends_on_past=False,
        query=f"""DELETE FROM {STAGING_TABLE}""",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    (
        create_staging_table >>
        ingest_to_staging_table >>
        is_latest_dag_run >>
        audit_staging_table >>
        create_production_table >>
        exchange_step >>
        cleanup_step
    )

load_pypi_dag()