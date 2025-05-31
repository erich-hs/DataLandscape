from datetime import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=["example", "lambda", "reddit"],
)
def fetch_reddit_dag():
    # Task to invoke the Lambda function
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="dl-dev-ingest-reddit-01",
        payload="""{"key": "value"}""",
        log_type="Tail",
        aws_conn_id="aws_invoke_lambda",
    )

    invoke_lambda


fetch_reddit_dag()
