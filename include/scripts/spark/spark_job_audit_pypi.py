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
        "target_table",
        "invalid_records_tolerance"
    ]
)

target_table = args["target_table"]
invalid_records_tolerance = float(args["invalid_records_tolerance"])


# %% Initialize Spark session
spark = SparkSession \
        .builder \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


# %% Spark job
# Read target table
pypi_df = spark.table(f"glue_catalog.mad_dashboard_dl.{target_table}")

# Duplicate records check
duplicate_records = pypi_df \
    .groupBy(
        "download_date",
        "project",
        "project_version",
        "python",
        "system_name",
        "country_code"
    ) \
    .count() \
    .where("count > 1")

# Pipeline fails if duplicate records are found
if duplicate_records.count() > 0:
    raise ValueError(f"Duplicate records found in table '{target_table}'.")

# Invalid records check
invalid_null_values = pypi_df \
    .where(
        """download_date IS NULL
        OR project IS NULL
        OR project_version IS NULL
        OR download_count IS NULL
        """
    )
invalid_download_counts = pypi_df.where("download_count <= 0")
invalid_country_codes = pypi_df.where("country_code NOT REGEXP '^[A-Z]{2}$'")

# Invalid records handling
invalid_null_values_count = invalid_null_values.count()
invalid_download_counts_count = invalid_download_counts.count()
invalid_country_codes_count = invalid_country_codes.count()
total_invalid_records_count = invalid_null_values_count + invalid_download_counts_count + invalid_country_codes_count

if total_invalid_records_count:
    invalid_records = invalid_null_values.union(invalid_download_counts).union(invalid_country_codes)

    # Write invalid records to _invalid table
    print(f"Total invalid records count: {total_invalid_records_count}")
    invalid_records \
        .write \
        .mode("append") \
        .format("iceberg") \
        .saveAsTable(f"glue_catalog.mad_dashboard_dl.{target_table}_invalid")
    print(f"Invalid records saved to table '{target_table}_invalid'.")

if invalid_records_tolerance > 0:
    invalid_records_ratio = total_invalid_records_count / pypi_df.count()

    if invalid_records_ratio > invalid_records_tolerance:
        raise ValueError(f"Invalid records ratio exceeds tolerance: {invalid_records_ratio:.3f}")
    else:
        print(f"Invalid records ratio is within tolerance: {invalid_records_ratio:.3f}")

job = Job(glueContext)
