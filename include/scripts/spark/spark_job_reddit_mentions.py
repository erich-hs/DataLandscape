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
    MapType,
    ArrayType,
    StructType,
    StructField
)
from guidance import system, user, assistant, gen
from guidance.models import OpenAI
from textblob import TextBlob


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
        "llm_temperature",
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
llm_temperature = float(args["llm_temperature"])


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
    mentions = []
    
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
            mentions.append(
                {
                    "project": term,
                    "mentions": len(matches_list)
                }
            )
    
    return mentions if mentions else None

def summarize_and_extract_polarity(
    mentions: List[Dict[str, int]],
    comment: str,
    model: str = 'gpt-3.5-turbo',
    temperature: float = 0.5,
    api_key: str = None
) -> Dict[str, str]:
    if mentions:
        projects = set([mention['project'] for mention in mentions])
    else:
        return None
    mentions_summaries = []

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
        lm += f"""In the following social media comment the user mentioned the following technologies: {projects}.
Provide a one or two-sentence long summary for each technology mentioned by the user, capturing the user perception, experience, or opinion about that technology.
Give emphasis to the overal sentiment of the user towards each technology if possible.

Social media comment:
"{comment}"
"""
    with assistant():
        lm += "{" + gen(name='json_str', stop="}", temperature=temperature) + "}"
    
    mentions_summary = json.loads("{" + lm['json_str'] + "}")
    if projects:
        for project in projects:
            if project in mentions_summary:
                blob = TextBlob(mentions_summary[project])
                polarity = blob.sentiment.polarity
                mentions_summaries.append(
                    {
                        "project": project,
                        "summary": mentions_summary[project],
                        "polarity": polarity
                    }
                )
    
    return mentions_summaries if mentions_summaries else None

# Register UDFs
mention_schema = StructType([
    StructField("project", StringType(), True),
    StructField("mentions", IntegerType(), True)
])
find_mentions_udf = udf(find_mentions, ArrayType(mention_schema))

mentions_polarity_schema = StructType([
    StructField("project", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("polarity", DoubleType(), True)
])
summarize_and_extract_polarity_udf = udf(summarize_and_extract_polarity, ArrayType(mentions_polarity_schema))


# %% Spark Job
# Read from Iceberg tables
logging.info(f"Reading from submissions table {submissions_table} and comments table {comments_table}...")
submissions_df = spark \
    .read \
    .table(f'glue_catalog.mad_dashboard_dl.{submissions_table}') \
    .where(f"created_date = '{current_date}'") \
    .limit(100) \
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
    .limit(100) \
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

logging.info(f"Summarizing mentions and calculating polarity of tracked projects from submissions and comments...")
reddit_projects_mentions_df = reddit_projects_mentions_df.withColumn(
    "projects_mentions_polarity",
    when(
        reddit_projects_mentions_df.projects_mentions.isNotNull(),
        summarize_and_extract_polarity_udf(
            reddit_projects_mentions_df.projects_mentions,
            reddit_projects_mentions_df.text,
            lit('gpt-3.5-turbo'),
            lit(llm_temperature),
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