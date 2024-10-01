from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.schemas.reddit import agg_reddit_daily_mentions_polarity_create_table_query
from include.scripts.athena.reddit import reddit_daily_aggregate_insert_query

START_DATE = datetime(2024, 6, 20)

PRODUCTION_TABLE = 'agg_reddit_daily_mentions_polarity'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

@dag(
    "aggregate_reddit",
    description=f"Aggregate Reddit daily mentions and polarity into the `{PRODUCTION_TABLE}` table with total mentions count and average polarity per project per day.",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=3,
    schedule_interval="@daily",
    catchup=True,
    tags=["athena", "reddit", "aggregate"]
)
def aggregate_reddit_dag():
    wait_for_reddit_table = ExternalTaskSensor(
        task_id="wait_for_reddit_table",
        external_dag_id="process_reddit",
        external_task_id="process_reddit_mentions"
    )

    create_daily_agg_reddit_table = AthenaOperator(
        task_id="create_daily_agg_reddit_table",
        depends_on_past=False,
        query=agg_reddit_daily_mentions_polarity_create_table_query(
            target_table=PRODUCTION_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{PRODUCTION_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_partition = AthenaOperator(
        task_id="truncate_partition",
        depends_on_past=False,
        query=f"""DELETE FROM {PRODUCTION_TABLE} WHERE created_date = DATE('{{{{ ds }}}}')""",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    aggregate_reddit = AthenaOperator(
        task_id="aggregate_reddit",
        depends_on_past=False,
        query=reddit_daily_aggregate_insert_query(
            target_table=PRODUCTION_TABLE,
            reference_date='{{ ds }}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    (
        wait_for_reddit_table
        >> create_daily_agg_reddit_table
        >> truncate_partition
        >> aggregate_reddit
    )

aggregate_reddit_dag()