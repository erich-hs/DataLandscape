import sys
from awsglue.utils import getResolvedOptions # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql import SparkSession # type: ignore

# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ds"
    ]
)

current_date = args["ds"]

# %% Initialize Spark session
spark = SparkSession \
        .builder \
        .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# %% Write Parquet files
# pypi_file_downloads
pypi_file_downloads_df = spark \
    .read \
    .table("glue_catalog.mad_dashboard_dl.pypi_file_downloads")

pypi_file_downloads_df \
    .repartition(4, "project") \
    .sortWithinPartitions("system_name", "country_code") \
    .write \
    .mode("overwrite") \
    .parquet(f"s3://mad-dashboard-s3-001/data/evidence/pypi_file_downloads/{current_date}")

job = Job(glueContext)