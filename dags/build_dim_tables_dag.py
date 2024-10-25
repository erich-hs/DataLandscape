from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.providers.amazon.aws.operators.athena import AthenaOperator # type: ignore
from datetime import datetime, timedelta
from include.schemas.dim_tables import (
    dim_tracked_subreddits_create_table_query,
    dim_tracked_projects_create_table_query,
    dim_polarity_categories_create_table_query
)
from include.reference import SUBREDDITS, TRACKED_PROJECTS_JSON, POLARITY_CATEGORIES

START_DATE = datetime(2024, 10, 23)

# AWS Variables
S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

DIM_TRACKED_SUBREDDIT_TABLE = 'dim_tracked_subreddits'
DIM_TRACKED_PROJECTS_TABLE = 'dim_tracked_projects'
DIM_POLARITY_CATEGORIES_TABLE = 'dim_polarity_categories'

insert_dim_tracked_projects_values = []
for project in TRACKED_PROJECTS_JSON:
    project_name = project['project_name']
    search_terms = ', '.join([f"'{term}'" for term in project['project_search_terms']])
    search_terms = f"ARRAY[{search_terms}]"
    pypi = f"'{project['pypi']}'" if project['pypi'] else 'NULL'
    categories = ', '.join([f"'{cat}'" for cat in project['project_categories']])
    categories = f"ARRAY[{categories}]"
    
    insert_dim_tracked_projects_values.append(f"('{project_name}', {search_terms}, {pypi}, {categories})")

insert_dim_polartiy_categories_values = []
for category in POLARITY_CATEGORIES:
    insert_dim_polartiy_categories_values.append(f"('{category}', {POLARITY_CATEGORIES[category][0]}, {POLARITY_CATEGORIES[category][1]})")

@dag(
    "build_dim_tables",
    description="Build Dimension Tables",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
    tags=["reddit", "pypi", "dimension"]
)
def build_dim_tables_dag():
    create_dim_tracked_subreddits_table = AthenaOperator(
        task_id="create_dim_tracked_subreddits_table",
        depends_on_past=False,
        query=dim_tracked_subreddits_create_table_query(
            target_table=DIM_TRACKED_SUBREDDIT_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{DIM_TRACKED_SUBREDDIT_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    create_dim_tracked_projects_table = AthenaOperator(
        task_id="create_dim_tracked_projects_table",
        depends_on_past=False,
        query=dim_tracked_projects_create_table_query(
            target_table=DIM_TRACKED_PROJECTS_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{DIM_TRACKED_PROJECTS_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    create_dim_polarity_categories_table = AthenaOperator(
        task_id="create_dim_polarity_categories_table",
        depends_on_past=False,
        query=dim_polarity_categories_create_table_query(
            target_table=DIM_POLARITY_CATEGORIES_TABLE,
            location=f'{S3_BUCKET}/data/mad_dashboard_dl/{DIM_POLARITY_CATEGORIES_TABLE}'
        ),
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_dim_tracked_subreddits_table = AthenaOperator(
        task_id="truncate_dim_tracked_subreddits_table",
        depends_on_past=False,
        query=f"DELETE FROM {DIM_TRACKED_SUBREDDIT_TABLE}",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    truncate_dim_tracked_projects_table = AthenaOperator(
        task_id="truncate_dim_tracked_projects_table",
        depends_on_past=False,
        query=f"DELETE FROM {DIM_TRACKED_PROJECTS_TABLE}",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )
    
    truncate_dim_polarity_categories_table = AthenaOperator(
        task_id="truncate_dim_polarity_categories_table",
        depends_on_past=False,
        query=f"DELETE FROM {DIM_POLARITY_CATEGORIES_TABLE}",
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    insert_into_dim_tracked_subreddits_table = AthenaOperator(
        task_id="insert_into_dim_tracked_subreddits_table",
        depends_on_past=False,
        query=f"""INSERT INTO {DIM_TRACKED_SUBREDDIT_TABLE} VALUES
    {', '.join([f"('{subreddit}', 'https://reddit.com/r/{subreddit.lower()}')" for subreddit in SUBREDDITS])}
        """,
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    insert_into_dim_tracked_projects_table = AthenaOperator(
        task_id="insert_into_dim_tracked_projects_table",
        depends_on_past=False,
        query=f"""INSERT INTO {DIM_TRACKED_PROJECTS_TABLE} VALUES
    {', '.join(insert_dim_tracked_projects_values)}
        """,
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    insert_into_dim_polarity_categories_table = AthenaOperator(
        task_id="insert_into_dim_polarity_categories_table",
        depends_on_past=False,
        query=f"""INSERT INTO {DIM_POLARITY_CATEGORIES_TABLE} VALUES
    {', '.join(insert_dim_polartiy_categories_values)}
        """,
        database="mad_dashboard_dl",
        output_location=f's3://{S3_BUCKET}/athena_results',
        sleep_time=30,
        region_name=AWS_DEFAULT_REGION
    )

    create_dim_tracked_subreddits_table >> truncate_dim_tracked_subreddits_table >> insert_into_dim_tracked_subreddits_table
    create_dim_tracked_projects_table >> truncate_dim_tracked_projects_table >> insert_into_dim_tracked_projects_table
    create_dim_polarity_categories_table >> truncate_dim_polarity_categories_table >> insert_into_dim_polarity_categories_table

build_dim_tables_dag()