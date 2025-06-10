from pendulum import datetime
from airflow.decorators import dag  # type: ignore
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)  # type: ignore


@dag(
    schedule=None,
    start_date=datetime(2025, 6, 6),
    catchup=False,
    tags=["reddit", "ingest", "s3"],
)
def ingest_reddit_to_s3_dag():
    ingest_reddit_from_api = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="dl-dev-ingest-reddit-01",
        payload="""{
  "reddit_client_id": "reddit_client_id",
  "reddit_client_secret": "reddit_client_secret",
  "tasks": [
    {
      "subreddit": "python",
      "s3_bucket": "dl-dev-s3bucket-landingzone01",
      "submission_limit": 5,
      "comment_replace_more_limit": null
    },
    {
      "subreddit": "datascience",
      "s3_bucket": "dl-dev-s3bucket-landingzone01",
      "submission_limit": 10,
      "comment_replace_more_limit": 1
    }
  ]
}""",
        log_type="Tail",
        aws_conn_id="aws_invoke_lambda",
    )

    ingest_reddit_from_api


ingest_reddit_to_s3_dag()
