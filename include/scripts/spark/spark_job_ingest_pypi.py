import time
import base64
import json
import sys
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from typing import List, Union
from awsglue.utils import getResolvedOptions # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql import SparkSession # type: ignore
from google.cloud import bigquery
from google.oauth2 import service_account


# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ds",
        "pypi_project",
        "target_table"
    ]
)

min_date = args["ds"]
max_date = datetime.strptime(min_date, "%Y-%m-%d") + timedelta(days=1)
max_date = max_date.strftime("%Y-%m-%d")
pypi_project = args["pypi_project"]

if len(pypi_project.split(",")) > 1:
    # Handle multiple PyPI projects
    pypi_project = pypi_project.split(",")

target_table = args["target_table"]


# %% Initialize Spark session
spark = SparkSession \
        .builder \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


# %% Define auxiliary functions
def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logging.error(f"Failed to fetch secret {secret_name} with exception {e}")
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

def pypi_file_downloads_query(
    pypi_project: Union[str, List[str]],
    min_date: str,
    max_date: str
) -> str:
    if isinstance(pypi_project, str):
        project_where_clause = f"project = '{pypi_project}'"
    elif isinstance(pypi_project, list):
        projects_str = ', '.join([f"'{p}'" for p in pypi_project])
        project_where_clause = f"project IN ({projects_str})"

    return f"""SELECT
    CAST(date_trunc(timestamp, DAY) AS DATE) AS download_date,
    project,
    file.version AS project_version,
    details.python AS python,
    details.system.name AS system_name,
    country_code,
    COUNT(project) AS download_count,
FROM `bigquery-public-data.pypi.file_downloads`
WHERE
    {project_where_clause}
    AND TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP("{min_date}")
    AND TIMESTAMP_TRUNC(timestamp, DAY) < TIMESTAMP("{max_date}")
GROUP BY
    CAST(date_trunc(timestamp, DAY) AS DATE),
    project,
    file.version,
    country_code,
    details.python,
    details.system.name
"""


# %% Fetch from BigQuery
bigquery_query = pypi_file_downloads_query(
    pypi_project=pypi_project,
    min_date=min_date,
    max_date=max_date
)

# Sadly a BigQuery connection in Glue did not work here, so the BigQuery Python SDK is used instead.
# The next lines of code will fetch the PyPI file downloads from BigQuery as a PyArrow Table and INSERT OVERWRITE
# into the target iceberg table. All of it, until the write is called, is being handled at the driver node.
# Spark is still being leveraged here to ensure that the downstream footprint is consistent as an Iceberg table.

# Retrieve BigQuery credentials
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name='us-west-2'
)

# BigQuery credentials
bigquery_credentials_secret = get_secret(secret_name = "BigQueryCredentials", region_name = "us-west-2")
bigquery_credentials_json = json.loads(base64.b64decode(json.loads(bigquery_credentials_secret)['credentials']))
bigquery_credentials = service_account.Credentials.from_service_account_info(bigquery_credentials_json)

# Instantiate BigQuery client
bigquery_client = bigquery.Client(project="mad-dashboard-app", credentials=bigquery_credentials)

# Fetch PyPI file downloads aggregates from BigQuery
logging.info(f"Fetching PyPI file downloads from BigQuery with query:\n{bigquery_query}")
start_time = time.time()
try:
    pypi_project_table = bigquery_client.query(bigquery_query, timeout=300).to_arrow()
except Exception as e:
    logging.error(f"Failed to fetch PyPI file downloads from BigQuery: {e}")
    raise e
end_time = time.time()
logging.info(f"{len(pypi_project_table)} records for PyPI file downloads fetched in {end_time - start_time} seconds.")


# %% Write to Iceberg table
# Instantiate a PySpark DataFrame from the PyArrow Table
pypi_project_df = spark.createDataFrame(pypi_project_table.to_pandas())

# Insert Overwrite DataFrame to the target table
pypi_project_df \
    .writeTo(f'glue_catalog.mad_dashboard_dl.{target_table}') \
    .using('iceberg') \
    .overwritePartitions()

job = Job(glueContext)
