import json
import re
import sys
from typing import Dict, List
import boto3
import logging
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import udf, lit, array, col, when # type: ignore
from pyspark.sql.types import (  # type: ignore
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    StructType,
    StructField
)
from pydantic import BaseModel
from openai import OpenAI


# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ds",
        "tracked_projects_json",
        "submissions_table",
        "comments_table",
        "target_table",
        "llm_model",
        "llm_max_completion_tokens",
        "llm_temperature",
        "llm_timeout"
    ]
)

current_date = args["ds"]
target_table = args["target_table"]
submissions_table = args["submissions_table"]
comments_table = args["comments_table"]
llm_model = args["llm_model"]
llm_max_completion_tokens = int(args["llm_max_completion_tokens"])
llm_temperature = float(args["llm_temperature"])
llm_timeout = int(args["llm_timeout"])

TRACKED_PROJECTS_JSON = json.loads(args["tracked_projects_json"])


# %% Initialize Spark session
spark = SparkSession \
        .builder \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# %% Define auxiliary functions
def get_secret(
    secret_name: str,
    region_name: str
) -> str:
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
        print(f"Failed to fetch secret {secret_name} with exception {e}")
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

def find_mentions(text: str) -> List[Dict[str, int]]:
    mentions = []
    
    # The TRACKED_PROJECTS_JSON is defined as a global variable and broadcasted via the Spark UDF
    for project in TRACKED_PROJECTS_JSON:
        project_name = project['project_name']
        project_search_terms = project['project_search_terms']

        for term in project_search_terms:
            # Split the term into words
            words = re.split(r'[-\s]+', term)
            
            # Match words with optional hyphens in-between
            pattern = r'\b' + r'[-\s]?'.join(re.escape(word) for word in words) + r'\b'
            
            # Find all matches
            matches = re.finditer(pattern, text, re.IGNORECASE)
            matches_list = [match.group() for match in matches]
            if matches_list:
                mentions.append(
                    {
                        "project": project_name,
                        "mentions": len(matches_list)
                    }
                )
    
    return mentions if mentions else None

def summarize_and_extract_polarity(
    mentions: List[Dict[str, int]],
    comment: str,
    model: str,
    max_completion_tokens: int = 1000,
    temperature: float = 0.5,
    timeout: int = 60,
    api_key: str = None
) -> List[Dict[str, str]]:
    if mentions:
        projects = set([mention['project'] for mention in mentions])
    else:
        return None

    mentions_polarities = []

    # OpenAI Structured Output schema
    class TechnologyPolarity(BaseModel):
        technology_name: str
        summary: str
        polarity: float

    class Polarities(BaseModel):
        polarities: list[TechnologyPolarity]

    client = OpenAI(api_key=api_key)

    target_schema = [{"technology_name": "Technology One", "summary": "Summarized user perception, experience, or opinion about Technology One in one or two sentences.", "polarity": 0.56}, {"technology_name": "Technology Two", "summary": "Summarized user perception, experience, or opinion about Technology Two in one or two sentences.", "polarity": -0.12}, {"technology_name": "Technology Three", "summary": "Summarized user perception, experience, or opinion about Technology Three in one or two sentences.", "polarity": 0.73}]

    system_prompt = f"""You are an expert at sentiment analysis and structured data extraction. You will be given unstructured text from a social media comment and a list of technology names mentioned in that text.
Your task is to summarize in one or two sentences the user perception, experience, or opinion about each technology from the list provided, and based on that summary calculate a polarity score between -1.00 and 1.00 for each technology, where -1.00 is extremely negative, 0.00 is neutral, and 1.00 is extremely positive.
The polarity score for a given technology should represent the overall sentiment of the user towards that technology.
You should only return summaries for the technologies present in the list provided.
You should return a list of JSON objects with the following format: {target_schema}"""
    
    user_prompt = f"""Technologies mentioned: {projects}.
Social media comment: {comment}"""

    completion = client.beta.chat.completions.parse(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_completion_tokens=max_completion_tokens,
        timeout=timeout,
        temperature=temperature,
        response_format=Polarities
    )

    for project in completion.choices[0].message.parsed.polarities:
        mentions_polarities.append(
            {
                "project": project.technology_name,
                "summary": project.summary,
                "polarity": project.polarity
            }
        )
    
    return mentions_polarities if mentions_polarities else None


# Register UDFs
mention_schema = StructType([
    StructField("project", StringType(), True),
    StructField("mentions", IntegerType(), True)
])
find_mentions_udf = udf(find_mentions, ArrayType(mention_schema))

mentions_polarities_schema = StructType([
    StructField("project", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("polarity", DoubleType(), True)
])
summarize_and_extract_polarity_udf = udf(summarize_and_extract_polarity, ArrayType(mentions_polarities_schema))


# %% Spark Job
# Read from Iceberg tables
logging.info(f"Reading from submissions table {submissions_table} and comments table {comments_table}...")
submissions_df = spark \
    .read \
    .table(f'glue_catalog.mad_dashboard_dl.{submissions_table}') \
    .where(f"created_date = '{current_date}'") \
    .select(
        lit('submission').alias('content_type'),
        col('created_utc'),
        col('created_date'),
        col('title'),
        col('id'),
        col('subreddit_name_prefixed').alias('subreddit'),
        col('selftext').alias('text'),
        col('permalink'),
        col('score')
    )

comments_df = spark \
    .read \
    .table(f'glue_catalog.mad_dashboard_dl.{comments_table}') \
    .where(f"created_date = '{current_date}'") \
    .select(
        lit('comment').alias('content_type'),
        col('created_utc'),
        col('created_date'),
        lit('').alias('title'),
        col('id'),
        col('subreddit_name_prefixed').alias('subreddit'),
        col('body').alias('text'),
        col('permalink'),
        col('score')
    )

# Union all
reddit_projects_mentions_df = submissions_df.union(comments_df)

# Extract projects mentions from submissions and comments
logging.info(f"Extracting mentions of tracked projects from submissions and comments...")
reddit_projects_mentions_df = reddit_projects_mentions_df.withColumn(
    "projects_mentions",
    find_mentions_udf(reddit_projects_mentions_df.text)
)


# Process polarity
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name='us-west-2'
)
openai_api_key_secret = get_secret(secret_name = "mad_dashboard/OpenAIAPIKey", region_name = "us-west-2")
openai_api_key = json.loads(openai_api_key_secret)['OPENAI_API_KEY']

logging.info(f"Summarizing mentions and calculating polarity of tracked projects from submissions and comments...")
reddit_projects_mentions_df = reddit_projects_mentions_df.withColumn(
    "projects_mentions_polarity",
    when(
        reddit_projects_mentions_df.projects_mentions.isNotNull(),
        summarize_and_extract_polarity_udf(
            reddit_projects_mentions_df.projects_mentions,
            reddit_projects_mentions_df.text,
            lit(llm_model),
            lit(llm_max_completion_tokens),
            lit(llm_temperature),
            lit(llm_timeout),
            lit(openai_api_key)
        )
    )
)


# Write to target table
reddit_projects_mentions_df \
    .writeTo(f'glue_catalog.mad_dashboard_dl.{target_table}') \
    .using('iceberg') \
    .overwritePartitions()

job = Job(glueContext)