import sys
import logging
from awsglue.utils import getResolvedOptions  # type: ignore
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from pyspark.sql import SparkSession  # type: ignore

# %% Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "database_name", "table_name", "branch_name"],
)

database_name = args["database_name"]
table_name = args["table_name"]
branch_name = args["branch_name"]

# %% Initialize Spark session
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

logging.info(f"Enabling WAP on table {database_name}.{table_name}")
spark.sql(
    f"ALTER TABLE {database_name}.{table_name} SET TBLPROPERTIES ('write.wap.enabled'='true')"
)
logging.info(f"Creating branch {branch_name} on table {database_name}.{table_name}")
spark.sql(
    f"ALTER TABLE {database_name}.{table_name} CREATE OR REPLACE BRANCH {branch_name}"
)
logging.info(f"Branch {branch_name} created on table {database_name}.{table_name}")

job = Job(glueContext)
