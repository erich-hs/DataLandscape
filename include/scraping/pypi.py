import os
import time
import logging
import boto3
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
import pyarrow as pa

from ..schemas.pypi import pypi_file_downloads_query, pypi_file_downloads_schema
from ..utils.log_config import setup_s3_logging
from ..utils.duck import DuckDBManager, write_table_to_s3

def query_bigquery(
    query: str,
    bigquery_client: bigquery.Client,
    logger: logging.Logger,
) -> pa.Table:
    logger.info(f"Fetching from BigQuery executing:\n{query}")
    time_start = time.time()
    table = bigquery_client.query(query).to_arrow()
    time_end = time.time()
    logger.info(f"{len(table)} rows retrieved in {time_end - time_start:.2f} seconds")
    return table

def fetch_pypi(
    bigquery_project: str,
    pypi_project: str,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
):
    try:
        # Read service account file location from environment variable
        service_account_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        # Instantiate BigQuery client
        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        bigquery_client = bigquery.Client(project=bigquery_project, credentials=credentials)

    except DefaultCredentialsError as e:
        logger.error("Failed to instantiate BigQuery client with exception {e}")
        raise e

    except EnvironmentError as e:
        logger.error("Failed to fetch service account credentials file with exception {e}")
        raise e

    # Query to fetch PyPI file downloads
    query = pypi_file_downloads_query(pypi_project, start_date, end_date)

    # Fetch PyPI file downloads from BigQuery
    table = query_bigquery(query, bigquery_client, logger)
    
    return table

if __name__ == "__main__":

    load_dotenv()
    s3_bucket = os.getenv("AWS_S3_BUCKET")
    s3_client = boto3.client('s3')

    logger = setup_s3_logging(
        logger_name='pypi_bigquery',
        s3_bucket=s3_bucket,
        s3_log_dir='logs',
        s3_client=s3_client,
        local_log_dir=f'.dev/logs'
    )

    duckdb_manager = DuckDBManager(
        database=".dev/data/dev.duckdb",
        logger=logger
    )

    pypi_table = fetch_pypi(
        bigquery_project="mad-dashboard-app",
        pypi_project="duckdb",
        start_date="2024-06-20",
        end_date="2024-06-21",
        logger=logger
    )

    write_table_to_s3(
        table=pypi_table,
        table_name="pypi_file_downloads",
        duckdb_schema=pypi_file_downloads_schema,
        duckdb_manager=duckdb_manager,
        s3_bucket=s3_bucket,
        format='parquet'
    )
