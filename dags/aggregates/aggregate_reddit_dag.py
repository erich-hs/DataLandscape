from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.schemas.reddit import (
    agg_reddit_daily_mentions_polarity_create_table_query,
    agg_reddit_cumulative_mentions_polarity_create_table_query
)
from include.scripts.athena.reddit import (
    reddit_daily_aggregate_insert_query,
    reddit_cumulative_aggregate_insert_query
)

START_DATE = datetime(2025, 1, 4)

DAILY_AGG_PRODUCTION_TABLE = 'agg_reddit_daily_mentions_polarity'
CUMULATIVE_AGG_PRODUCTION_TABLE = 'agg_reddit_cumulative_mentions_polarity'

S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

@dag(
    "aggregate_reddit",
    description=f"Aggregate Reddit daily and cumulative mentions and average polarity into the `{DAILY_AGG_PRODUCTION_TABLE}` and `{CUMULATIVE_AGG_PRODUCTION_TABLE}` tables.",
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
            target_table=DAILY_AGG_PRODUCTION_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{DAILY_AGG_PRODUCTION_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    create_cumulative_agg_reddit_table = AthenaOperator(
        task_id="create_cumulative_agg_reddit_table",
        depends_on_past=False,
        query=agg_reddit_cumulative_mentions_polarity_create_table_query(
            target_table=CUMULATIVE_AGG_PRODUCTION_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{CUMULATIVE_AGG_PRODUCTION_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_daily_agg_partition = AthenaOperator(
        task_id="truncate_daily_agg_partition",
        depends_on_past=False,
        query=f"""DELETE FROM {DAILY_AGG_PRODUCTION_TABLE} WHERE created_date = DATE('{{{{ ds }}}}')""",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_cumulative_agg_partition = AthenaOperator(
        task_id="truncate_cumulative_agg_partition",
        depends_on_past=False,
        query=f"""DELETE FROM {CUMULATIVE_AGG_PRODUCTION_TABLE} WHERE reference_date = DATE('{{{{ ds }}}}')""",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    aggregate_reddit_daily = AthenaOperator(
        task_id="aggregate_reddit_daily",
        depends_on_past=False,
        query=reddit_daily_aggregate_insert_query(
            target_table=DAILY_AGG_PRODUCTION_TABLE,
            reference_date='{{ ds }}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    aggregate_reddit_cumulative = AthenaOperator(
        task_id="aggregate_reddit_cumulative",
        depends_on_past=True,
        query=reddit_cumulative_aggregate_insert_query(
            target_table=CUMULATIVE_AGG_PRODUCTION_TABLE,
            reference_date='{{ ds }}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    optimize_daily_agg_table = AthenaOperator(
        task_id="optimize_daily_agg_table",
        depends_on_past=False,
        query=f"OPTIMIZE {DAILY_AGG_PRODUCTION_TABLE} REWRITE DATA USING BIN_PACK",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    optimize_cumulative_agg_table = AthenaOperator(
        task_id="optimize_cumulative_agg_table",
        depends_on_past=False,
        query=f"OPTIMIZE {CUMULATIVE_AGG_PRODUCTION_TABLE} REWRITE DATA USING BIN_PACK",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=10,
        region_name=AWS_DEFAULT_REGION
    )

    (
        wait_for_reddit_table
        >> create_daily_agg_reddit_table
        >> truncate_daily_agg_partition
        >> aggregate_reddit_daily
        >> aggregate_reddit_cumulative
        >> [
            optimize_daily_agg_table,
            optimize_cumulative_agg_table
        ]
    )
    
    (
        wait_for_reddit_table
        >> create_cumulative_agg_reddit_table
        >> truncate_cumulative_agg_partition
        >> aggregate_reddit_cumulative
        >> [
            optimize_daily_agg_table,
            optimize_cumulative_agg_table
        ]
    )

aggregate_reddit_dag()