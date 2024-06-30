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
from pyspark.sql.functions import udf, lit, array, col # type: ignore
from pyspark.sql.types import (  # type: ignore
    StringType,
    IntegerType,
    MapType,
)
from guidance import system, user, assistant, gen
from guidance.models import OpenAI


# %% Build local arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ds",
        "tracked_projects",
        "submissions_table",
        "comments_table",
        "target_table",
    ]
)

current_date = args["ds"]
tracked_projects = args["tracked_projects"]

if len(tracked_projects.split(",")) > 1:
    # Handle multiple PyPI projects
    tracked_projects = tracked_projects.split(",")

target_table = args["target_table"]
submissions_table = args["submissions_table"]
comments_table = args["comments_table"]


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
        print(f"Failed to fetch secret {secret_name} with exception {e}")
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

def find_mentions(text, look_for):
    mentions = {}
    
    for term in look_for:
        # Split the term into words
        words = re.split(r'[-\s]+', term)
        
        # Create a regex pattern that matches words with optional hyphens between them
        pattern = r'\b' + r'[-\s]?'.join(re.escape(word) for word in words) + r'\b'
        
        # Find all matches
        matches = re.finditer(pattern, text, re.IGNORECASE)
        
        # Store matches
        matches_list = [match.group() for match in matches]
        if matches_list:
            mentions[term] = len(matches_list)
    
    return mentions if mentions else None

def summarize_mentions(
    mentions: str,
    comment: List[str],
    model: str = 'gpt-3.5-turbo',
    api_key: str = None
) -> Dict[str, str]:
    if api_key:
        lm = OpenAI(model, api_key=api_key, echo=False)
    else:
        lm = OpenAI(model, echo=False)

    with system():
        lm += """You always respond with json objects with no identation. You use the following format:
{"technology one": "Summarized user perception, experience, or opinion about technology one in one or two sentences.", "technology two": "Summarized user perception, experience, or opinion about technology two in one or two sentences.", "technology three": "Summarized user perception, experience, or opinion about technology three in one or two sentences."}
    You never return a json record for a technology that was not specifically indicated at the user prompt.
    You never enclose the json object in a list or array.
    You never enclose the json object in quotes.
"""
    with user():
        lm += f"""In the following social media comment the user mentioned the following technologies: {mentions}.
Provide a one or two-sentence long summary for each technology mentioned by the user, capturing the user perception, experience, or opinion about that technology.
Give emphasis to the overal sentiment of the user towards each technology if possible.

Social media comment:
"{comment}"
"""
    with assistant():
        lm += "{" + gen(name='json_str', stop="}") + "}"
    
    return json.loads("{" + lm['json_str'] + "}")

# Register UDFs
find_mentions_udf = udf(find_mentions, MapType(StringType(), IntegerType()))
summarize_mentions_udf = udf(summarize_mentions, MapType(StringType(), IntegerType()))


# %% Spark Job
# Read from Iceberg tables
submissions_df = spark \
    .read \
    .table(f'glue_catalog.mad_dashboard_dl.{submissions_table}') \
    .where(f"created_date = '{current_date}'") \
    .select(
        lit('submission').alias('type'),
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
        lit('comment').alias('type'),
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
reddit_projects_mentions_df = reddit_projects_mentions_df.withColumn(
    "projects_mentions",
    find_mentions_udf(
        reddit_projects_mentions_df.text, 
        array(*[lit(term) for term in tracked_projects])
    )
)

# Process polarity
# Retrieve OpenAI API key
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name='us-west-2'
)
openai_api_key_secret = get_secret(secret_name = "mad_dashboard/OpenAIAPIKey", region_name = "us-west-2")
openai_api_key = json.loads(openai_api_key_secret)['OPENAI_API_KEY']

reddit_projects_mentions_df = reddit_projects_mentions_df.withColumn(
    "projects_mentions_summary",
    summarize_mentions_udf(
        reddit_projects_mentions_df.projects_mentions,
        reddit_projects_mentions_df.text,
        lit('gpt-3.5-turbo'),
        lit(openai_api_key)
    )
)

# Write to target table
reddit_projects_mentions_df \
    .writeTo(f'glue_catalog.mad_dashboard_dl.{target_table}') \
    .using('iceberg') \
    .overwritePartitions()

job = Job(glueContext)