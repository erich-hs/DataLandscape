import sys
import boto3  # type: ignore
import logging
from awsglue.utils import getResolvedOptions  # type: ignore
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import (  # type: ignore
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DoubleType,
    ArrayType,
    MapType,
    DateType,
)
from pyspark.sql.functions import current_timestamp  # type: ignore

# %% Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ds",
        "target_submissions_table",
        "target_comments_table",
        "target_database",
        "source_s3_bucket",
        "source_dir",
    ],
)

current_date = args["ds"]
target_submissions_table = args["target_submissions_table"]
target_comments_table = args["target_comments_table"]
target_database = args["target_database"]
source_s3_bucket = args["source_s3_bucket"]
source_dir = args["source_dir"]


# %% Initialize Spark session
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "50").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


# %% Define auxiliary functions
def list_all_files(bucket, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        if "Contents" in page:
            files.extend([content["Key"] for content in page["Contents"]])
    return files


# %% Submissions and Comments schema
submissions_schema = StructType(
    [
        StructField("archived", BooleanType(), True),
        StructField("author_fullname", StringType(), True),
        StructField("author_flair_css_class", StringType(), True),
        StructField("author_flair_text", StringType(), True),
        StructField("category", StringType(), True),
        StructField("clicked", BooleanType(), True),
        StructField("created", DoubleType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("distinguished", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("downs", IntegerType(), True),
        StructField("edited", StringType(), True),
        StructField("gilded", IntegerType(), True),
        StructField("gildings", MapType(StringType(), IntegerType()), True),
        StructField("hidden", BooleanType(), True),
        StructField("hide_score", BooleanType(), True),
        StructField("id", StringType(), True),
        StructField("is_created_from_ads_ui", BooleanType(), True),
        StructField("is_meta", BooleanType(), True),
        StructField("is_original_content", BooleanType(), True),
        StructField("is_reddit_media_domain", BooleanType(), True),
        StructField("is_self", BooleanType(), True),
        StructField("is_video", BooleanType(), True),
        StructField("link_flair_css_class", StringType(), True),
        StructField("link_flair_text", StringType(), True),
        StructField("locked", BooleanType(), True),
        StructField("media", StringType(), True),
        StructField("media_embed", StringType(), True),
        StructField("media_only", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("no_follow", BooleanType(), True),
        StructField("num_comments", IntegerType(), True),
        StructField("num_crossposts", IntegerType(), True),
        StructField("over_18", BooleanType(), True),
        StructField("permalink", StringType(), True),
        StructField("pinned", BooleanType(), True),
        StructField("pwls", IntegerType(), True),
        StructField("quarantine", BooleanType(), True),
        StructField("removed_by_category", StringType(), True),
        StructField("saved", BooleanType(), True),
        StructField("score", IntegerType(), True),
        StructField("secure_media", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("selftext_html", StringType(), True),
        StructField("send_replies", BooleanType(), True),
        StructField("spoiler", BooleanType(), True),
        StructField("stickied", BooleanType(), True),
        StructField("subreddit_id", StringType(), True),
        StructField("subreddit_name_prefixed", StringType(), True),
        StructField("subreddit_subscribers", IntegerType(), True),
        StructField("subreddit_type", StringType(), True),
        StructField("thumbnail", StringType(), True),
        StructField("title", StringType(), True),
        StructField("total_awards_received", IntegerType(), True),
        StructField("treatment_tags", ArrayType(StringType()), True),
        StructField("ups", IntegerType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("url", StringType(), True),
        StructField("user_reports", ArrayType(StringType()), True),
        StructField("wls", IntegerType(), True),
        StructField("_fetched_date", DateType(), True),
        StructField("_fetched_iso_utc", StringType(), True),
    ]
)

comments_schema = StructType(
    [
        StructField("archived", BooleanType(), True),
        StructField("author_fullname", StringType(), True),
        StructField("author_flair_css_class", StringType(), True),
        StructField("author_flair_text", StringType(), True),
        StructField("awarders", ArrayType(StringType()), True),
        StructField("body", StringType(), True),
        StructField("body_html", StringType(), True),
        StructField("collapsed", BooleanType(), True),
        StructField("collapsed_reason", StringType(), True),
        StructField("collapsed_reason_code", StringType(), True),
        StructField("controversiality", IntegerType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("distinguished", StringType(), True),
        StructField("downs", IntegerType(), True),
        StructField("edited", StringType(), True),
        StructField("gilded", IntegerType(), True),
        StructField("gildings", MapType(StringType(), IntegerType()), True),
        StructField("id", StringType(), True),
        StructField("is_submitter", BooleanType(), True),
        StructField("link_id", StringType(), True),
        StructField("locked", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("no_follow", BooleanType(), True),
        StructField("parent_id", StringType(), True),
        StructField("permalink", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("score_hidden", BooleanType(), True),
        StructField("send_replies", BooleanType(), True),
        StructField("stickied", BooleanType(), True),
        StructField("subreddit_id", StringType(), True),
        StructField("subreddit_name_prefixed", StringType(), True),
        StructField("subreddit_type", StringType(), True),
        StructField("total_awards_received", IntegerType(), True),
        StructField("treatment_tags", ArrayType(StringType()), True),
        StructField("ups", IntegerType(), True),
        StructField("_fetched_date", DateType(), True),
        StructField("_fetched_iso_utc", StringType(), True),
    ]
)

# Create Namespace if it doesn't exist
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_database}")

# Create Iceberg tables if they don't exist
logging.info(
    f"Creating Iceberg tables if they don't exist: {target_submissions_table} and {target_comments_table}"
)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_database}.{target_submissions_table} (
    {", ".join([f"{field.name} {field.dataType.simpleString()}" for field in submissions_schema.fields])},
    _created_date DATE,
    _loaded_ts_utc TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (MONTH(_created_date))
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_database}.{target_comments_table} (
    {", ".join([f"{field.name} {field.dataType.simpleString()}" for field in comments_schema.fields])},
    _created_date DATE,
    _loaded_ts_utc TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (MONTH(_created_date))
""")
logging.info(f"Iceberg table {target_submissions_table} created")

# List comments and submission files
logging.info(f"Listing all files at {source_s3_bucket}/{source_dir}")
json_files = list_all_files(source_s3_bucket, source_dir)
submission_files = [
    f"s3://{source_s3_bucket}/{f}"
    for f in json_files
    if f.endswith("_submissions.json")
]
comments_files = [
    f"s3://{source_s3_bucket}/{f}" for f in json_files if f.endswith("_comments.json")
]
logging.info(
    f"Found {len(submission_files)} submission files and {len(comments_files)} comment files"
)

# Load files to dataframes
source_submissions_df = spark.read.schema(submissions_schema).json(submission_files)

source_comments_df = spark.read.schema(comments_schema).json(comments_files)

logging.info(
    f"Files successfully loaded from source: {source_submissions_df.count()} submissions and {source_comments_df.count()} comments"
)


# Build _created_date & _loaded_ts_utc columns
source_submissions_df = source_submissions_df.withColumns(
    {
        "_created_date": source_submissions_df.created_utc.cast("timestamp").cast(
            "date"
        ),
        "_loaded_ts_utc": current_timestamp(),
    }
)

source_comments_df = source_comments_df.withColumns(
    {
        "_created_date": source_comments_df.created_utc.cast("timestamp").cast("date"),
        "_loaded_ts_utc": current_timestamp(),
    }
)

# Write to iceberg tables
source_submissions_df.createOrReplaceTempView("submissions")
source_comments_df.createOrReplaceTempView("comments")

logging.info(
    f"Writing to {target_database}.{target_submissions_table} and {target_database}.{target_comments_table}"
)
spark.sql(
    f"""MERGE INTO {target_database}.{target_submissions_table} t
    USING submissions s
    ON (s.id = t.id)
    WHEN MATCHED
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
    """
)
spark.sql(
    f"""MERGE INTO {target_database}.{target_comments_table} t
    USING comments s
    ON (s.id = t.id)
    WHEN MATCHED
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
    """
)

logging.info(
    f"Data successfully written to Iceberg tables: {target_submissions_table} and {target_comments_table}"
)

job = Job(glueContext)
