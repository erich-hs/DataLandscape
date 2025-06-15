from pendulum import datetime
from airflow.sdk import Asset, chain
from airflow.decorators import dag  # type: ignore

reddit_raw_files_s3 = Asset(
    "reddit_raw_files_s3",
    uri="s3://dl-dev-s3bucket-landingzone01/data/reddit/api",
    group="reddit",
)


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 6),
    catchup=False,
    tags=["reddit", "ingest", "s3"],
)
def ingest_reddit_to_s3_dag():
    import json
    import time
    from airflow.providers.amazon.aws.operators.lambda_function import (
        LambdaInvokeFunctionOperator,
    )  # type: ignore
    from airflow.operators.python import PythonOperator  # type: ignore
    from airflow.operators.empty import EmptyOperator  # type: ignore

    SUBREDDITS = [
        "dataengineering",
        "MachineLearning",
        "datascience",
        "analytics",
        "LocalLLaMA",
        "learnprogramming",
    ]
    REDDIT_CLIENT_ID = "reddit_client_id"
    REDDIT_CLIENT_SECRET = "reddit_client_secret"
    S3_BUCKET = "dl-dev-s3bucket-landingzone01"
    SUBMISSION_LIMIT = 100
    COMMENT_REPLACE_MORE_LIMIT = None

    tasks = []
    for i, subreddit in enumerate(SUBREDDITS):
        invoke_lambda_task = LambdaInvokeFunctionOperator(
            task_id=f"{i + 1}_invoke_lambda_{subreddit}",
            function_name="dl-dev-ingest-reddit-01",
            payload=json.dumps(
                {
                    "reddit_client_id": REDDIT_CLIENT_ID,
                    "reddit_client_secret": REDDIT_CLIENT_SECRET,
                    "tasks": [
                        {
                            "subreddit": subreddit,
                            "s3_bucket": S3_BUCKET,
                            "submission_limit": SUBMISSION_LIMIT,
                            "comment_replace_more_limit": COMMENT_REPLACE_MORE_LIMIT,
                        }
                    ],
                }
            ),
            log_type="Tail",
            aws_conn_id="aws_invoke_lambda",
        )
        tasks.append(invoke_lambda_task)

        if i < len(SUBREDDITS) - 1:
            sleep_task = PythonOperator(
                task_id=f"{i + 1}_sleep_60s_after_{subreddit}",
                python_callable=lambda: time.sleep(60),
            )

            tasks.append(sleep_task)

    emmit_asset_task = EmptyOperator(
        task_id="emmit_asset", outlets=[reddit_raw_files_s3]
    )

    if tasks:
        chain(*tasks, emmit_asset_task)


ingest_reddit_to_s3_dag()
