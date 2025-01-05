from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.schemas.pypi import agg_pypi_cumulative_file_downloads_create_table_query
from include.scripts.athena.pypi import pypi_cumulative_aggregate_insert_query

START_DATE = datetime(2025, 1, 5)

PRODUCTION_TABLE = 'agg_pypi_cumulative_file_downloads'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')


@dag(
    "aggregate_pypi",
    description=f"Aggregate PyPI file downloads into the `{PRODUCTION_TABLE}` table with total download count per project, per project version, per country for the last seven and last thirty days.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=True,
    tags=["athena", "pypi", "aggregate"]
)
def aggregate_pypi_dag():
    wait_for_pypi_table = ExternalTaskSensor(
        task_id="wait_for_pypi_table",
        external_dag_id="load_pypi",
        external_task_id="exchange_step"
    )

    create_cumulative_agg_pypi_table = AthenaOperator(
        task_id="create_cumulative_agg_pypi_table",
        depends_on_past=False,
        query=agg_pypi_cumulative_file_downloads_create_table_query(
            target_table=PRODUCTION_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{PRODUCTION_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_cumulative_agg_partition = AthenaOperator(
        task_id="truncate_cumulative_agg_partition",
        depends_on_past=False,
        query=f"""DELETE FROM {PRODUCTION_TABLE} WHERE reference_date = DATE('{{{{ ds }}}}')""",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    aggregate_and_insert_data = AthenaOperator(
        task_id="aggregate_and_insert_data",
        depends_on_past=True,
        query=pypi_cumulative_aggregate_insert_query(target_table=PRODUCTION_TABLE, reference_date='{{ ds }}'),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    (
        wait_for_pypi_table
        >> create_cumulative_agg_pypi_table
        >> truncate_cumulative_agg_partition
        >> aggregate_and_insert_data
    )

aggregate_pypi_dag()