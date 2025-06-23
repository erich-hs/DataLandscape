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
    ["JOB_NAME"],
)

# %% Initialize Spark session
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "50").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

dataDF = spark.sql("""SELECT
    _created_date,
    COUNT(*) AS submission_count
FROM raw.reddit__submissions
GROUP BY _created_date
""")

dataDF.createOrReplaceTempView("stg_first_model")

spark.sql("""MERGE INTO raw.first_model AS target
USING stg_first_model AS source
    ON target._created_date = source._created_date
WHEN MATCHED THEN
    UPDATE SET target.submission_count = source.submission_count
WHEN NOT MATCHED THEN
    INSERT (
        _created_date,
        submission_count
    ) VALUES (
        source._created_date,
        source.submission_count
    );
""")

job = Job(glueContext)
