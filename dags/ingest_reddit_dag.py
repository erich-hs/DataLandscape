from airflow.decorators import dag # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
from include.scripts.scraping_job_reddit import fetch_reddit
from include.reference import SUBREDDITS

START_DATE = datetime(2024, 7, 25)

# AWS Variables
S3_BUCKET = Variable.get('AWS_S3_BUCKET')
AWS_ACCES_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = Variable.get('AWS_DEFAULT_REGION')

# Reddit API Variables
REDDIT_CLIENT_ID = Variable.get('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = Variable.get('REDDIT_CLIENT_SECRET')

@dag(
    "ingest_reddit",
    description="Ingest Reddit Submissions and Comments from the Reddit API",
    default_args={
        "owner": "Erich Silva",
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    tags=["scrapping", "reddit", "ingest"]
)
def ingest_reddit_dag():
    fetch_reddit_task = PythonOperator(
        task_id="fetch_reddit",
        python_callable=fetch_reddit,
        op_kwargs={
            "subreddits": SUBREDDITS,
            "s3_bucket": S3_BUCKET,
            "aws_access_key_id": AWS_ACCES_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "reddit_client_id": REDDIT_CLIENT_ID,
            "reddit_client_secret": REDDIT_CLIENT_SECRET,
            "local_logs_dir": None
        }
    )

    fetch_reddit_task

ingest_reddit_dag()