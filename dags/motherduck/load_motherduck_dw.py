from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from include.utils.motherduck import load_iceberg_table_to_motherduck
from include.schemas.pypi import (
    agg_pypi_cumulative_file_downloads_create_motherduck_table_query,
    agg_pypi_cumulative_file_downloads_pyarrow_schema,
    agg_pypi_daily_file_downloads_create_motherduck_table_query,
    agg_pypi_daily_file_downloads_pyarrow_schema
)
from datetime import datetime, timedelta

START_DATE = datetime(2025, 1, 5)

# Objects to be loaded
MOTHERDUCK_DATABASE = "data_landscape_dw"
MOTHERDUCK_RETENTION_DAYS = 30
MOTHERDUCK_TABLES = {
    "agg_pypi_cumulative_file_downloads": {
        "database_schema": "pypi",
        "athena_dql_query": "SELECT * FROM agg_pypi_cumulative_file_downloads WHERE reference_date = DATE('{{ ds }}')",
        "motherduck_ddl_query": agg_pypi_cumulative_file_downloads_create_motherduck_table_query(f"{MOTHERDUCK_DATABASE}.pypi.agg_pypi_cumulative_file_downloads"),
        "motherduck_preload_query": None,
        "motherduck_postload_query": f"DELETE FROM {MOTHERDUCK_DATABASE}.pypi.agg_pypi_cumulative_file_downloads WHERE reference_date < DATE('{{ macros.ds_add(ds, -{MOTHERDUCK_RETENTION_DAYS}) }}')",
        "pa_schema": agg_pypi_cumulative_file_downloads_pyarrow_schema
    },
    "agg_pypi_daily_file_downloads": {
        "database_schema": "pypi",
        "athena_dql_query": f"""SELECT
    download_date,
    project,
    SUM(download_count) AS download_count
FROM pypi_file_downloads
WHERE download_date <= DATE('{{ ds }}')
GROUP BY download_date, project
""",
        "motherduck_ddl_query": agg_pypi_daily_file_downloads_create_motherduck_table_query(f"{MOTHERDUCK_DATABASE}.pypi.agg_pypi_daily_file_downloads"),
        "motherduck_preload_query": f"DELETE FROM {MOTHERDUCK_DATABASE}.pypi.agg_pypi_daily_file_downloads",
        "motherduck_postload_query": None,
        "pa_schema": agg_pypi_daily_file_downloads_pyarrow_schema
    }
}

# AWS Variables
S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

# MotherDuck Variables
MOTHERDUCK_TOKEN = Variable.get('MOTHERDUCK_TOKEN')

def run_load_table_to_motherduck_task(table: str) -> PythonOperator:
    return PythonOperator(
        task_id=f"load_{table}_to_motherduck",
        python_callable=load_iceberg_table_to_motherduck,
        op_kwargs={
            "table": table,
            "database_schema": MOTHERDUCK_TABLES[table]["database_schema"],
            "athena_dql_query": MOTHERDUCK_TABLES[table]["athena_dql_query"],
            "motherduck_ddl_query": MOTHERDUCK_TABLES[table]["motherduck_ddl_query"],
            "pa_schema": MOTHERDUCK_TABLES[table]["pa_schema"],
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_default_region": AWS_DEFAULT_REGION,
            "motherduck_token": MOTHERDUCK_TOKEN
        },
        max_active_tis_per_dag=1
    )

@dag(
    "load_motherduck_dw",
    description="Load MotherDuck Data Warehouse from Iceberg tables",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    tags=["motherduck", "ingest", "athena"]
)
def load_motherduck_dw_dag():
    task_list = []
    task_list.append(run_load_table_to_motherduck_task(list(MOTHERDUCK_TABLES.keys())[0]))
    for table in list(MOTHERDUCK_TABLES.keys())[1:]:
        load_table_to_motherduck_task = run_load_table_to_motherduck_task(table)
        load_table_to_motherduck_task.set_upstream(task_list[-1])
        task_list.append(load_table_to_motherduck_task)

load_motherduck_dw_dag()