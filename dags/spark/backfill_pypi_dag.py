from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.utils.aws_glue import submit_glue_job
from include.reference import PYPI_PROJECTS

START_DATE = datetime(2024, 1, 1)

PRODUCTION_TABLE = 'pypi_file_downloads'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

def pypi_create_table_query(target_table):
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
        download_date DATE,
        project STRING,
        project_version STRING,
        python STRING,
        system_name STRING,
        country_code STRING,
        download_count INT
    )
    PARTITIONED BY (download_date)
    LOCATION 's3://{S3_BUCKET}/data/mad_dashboard_dl/{target_table}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy'
    )
    """

@dag(
    "backfill_pypi",
    description="Backfill PyPI file download counts from Google BigQuery public dataset.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=True,
    tags=["glue", "pypi", "backfill"]
)
def backfill_pypi_dag():
    ingest_pypi_script_path = 'include/scripts/spark/spark_job_ingest_pypi.py'

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
    create_production_table = AthenaOperator(
        task_id="create_production_table",
        depends_on_past=False,
        query=pypi_create_table_query(PRODUCTION_TABLE),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    ingest_to_production_table = PythonOperator(
        task_id="ingest_to_production_table",
        depends_on_past=False,
        python_callable=submit_glue_job,
        op_kwargs={
            "job_name": 'ingest_pypi_file_downloads',
            "job_description": 'Ingest aggregate PyPI file download counts from Google BigQuery public dataset.',
            "run_arguments": {
                "--ds": "{{ ds }}",
                "--pypi_project": ",".join(PYPI_PROJECTS),
                "--target_table": PRODUCTION_TABLE
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

    (
        create_production_table >>
        ingest_to_production_table
    )

backfill_pypi_dag()