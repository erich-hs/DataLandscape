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

# reddit_projects_mentions
# reddit_projects_mentions_df = spark \
#     .read \
#     .table("glue_catalog.mad_dashboard_dl.reddit_projects_mentions")


# pypi_file_downloads_df \
#     .select("download_date", "project", "download_count") \
#     .groupBy("download_date", "project") \
#     .agg({"download_count": "sum"}) \
#     .withColumnRenamed("sum(download_count)", "download_count") \
#     .coalesce(1) \
#     .write \
#     .mode("overwrite") \
#     .parquet(f"s3://mad-dashboard-s3-001/data/evidence/pypi_file_downloads/{current_date}")

# for table in source_tables:
#     # Read Iceberg table
#     df = spark \
#         .read \
#         .table(f'glue_catalog.mad_dashboard_dl.{table}')
    
#     # Write parquet
#     df \
#         .coalesce(1) \
#         .write \
#         .mode('overwrite') \
#         .parquet(f"s3://mad-dashboard-s3-001/data/evidence/{table}/{current_date}")

job = Job(glueContext)