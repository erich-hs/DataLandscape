import os
import sys
import logging
from awsglue.utils import getResolvedOptions  # type: ignore
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from pyspark.sql import SparkSession  # type: ignore

os.environ["SPARK_VERSION"] = "3.5"

from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Size, Completeness  # type: ignore

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

auditBranchDF = spark.sql("""SELECT
    *
FROM raw.first_model
""")

mainBranchDF = spark.sql("""SELECT
    *
FROM raw.first_model VERSION AS OF 'main'
""")

logging.info(f"Audit branch records count: {auditBranchDF.count()}")

analysisResult = (
    AnalysisRunner(spark)
    .onData(auditBranchDF)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("_created_date"))
    .run()
)

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

job = Job(glueContext)
