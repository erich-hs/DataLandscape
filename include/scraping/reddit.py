import os
import time
import json
import boto3
import logging
import praw
from typing import List, Tuple, Union
from datetime import datetime, UTC
from pydantic import ValidationError
from dotenv import load_dotenv

from ..schemas.reddit import RedditSubmission, RedditComment
from ..utils.s3 import put_to_s3
from ..utils.log_config import setup_s3_logging

SUBREDDITS = [
    "dataengineering",
    "MachineLearning",
    "datascience",
    "analytics",
    "LocalLLaMA",
    "learnprogramming",
]

def fetch_subreddit_newest_submissions(
    reddit: praw.reddit.Reddit,
    subreddit_name: str,
    logger: logging.Logger,
    limit: int=100,
) -> Tuple[List[RedditSubmission], List[dict]]:
    # Instantiate subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime('%Y-%m-%d')

    # Fetch newest submissions
    top_new = subreddit.new(limit=limit)

    # Initialize list for validated and invalid submissions
    submissions: List[RedditSubmission] = []
    invalid_submissions: List[Tuple[dict, str]] = []

    for submission in top_new:
        try:
            # Validate submission
            validated_submission = RedditSubmission(**submission.__dict__)
            validated_submission_dict = dict(validated_submission)

            # Add fetched timestamp and date
            validated_submission_dict['_fetched_date'] = _fetched_date
            validated_submission_dict['_fetched_iso_utc'] = _fetched_iso_utc

            # Convert edited to None if False as the value is not a timestamp
            if not validated_submission_dict['edited']:
                validated_submission_dict['edited'] = None

            submissions.append(validated_submission_dict)

        except ValidationError as e:
            invalid_submission = {"invalid_record": submission.__dict__, "error": e.__str__()}
            logger.warning(f"Failed to validate submission: {invalid_submission} with exception {e}")
            invalid_submissions.append(invalid_submission)
        
        except praw.exceptions.RedditAPIException as e:
            logger.error(f"Request failed while fetching submission with server exception {e}")
            raise e
        
        except praw.exceptions.ClientException as e:
            logger.error(f"Request failed while fetching submission with client exception {e}")
            raise e
    
    logger.info(f"Successfully fetched {len(submissions)} submissions from r/{subreddit_name}")
    if invalid_submissions:
        logger.warning(f"Failed to validate {len(invalid_submissions)} submissions from r/{subreddit_name}")
    
    return submissions, invalid_submissions

def fetch_submission_comments(
    reddit: praw.reddit.Reddit,
    submission: Union[praw.models.Submission, str],
    logger: logging.Logger,
    limit: Union[int, None]=None,
) -> Tuple[List[RedditComment], List[dict]]:
    if isinstance(submission, str):
        submission = reddit.submission(submission)
    
    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime('%Y-%m-%d')

    # Retrieve comments recursively
    submission.comments.replace_more(limit=limit)

    # Initialize list for validated and invalid submissions
    comments: List[RedditComment] = []
    invalid_comments: List[Tuple[dict, str]] = []

    for comment in submission.comments.list():
        try:
            # Validate comment
            validated_comment = RedditComment(**comment.__dict__)
            validated_comment_dict = dict(validated_comment)

            # Add fetched timestamp and date
            validated_comment_dict['_fetched_date'] = _fetched_date
            validated_comment_dict['_fetched_iso_utc'] = _fetched_iso_utc
            
            # Convert edited to None if False as the value is not a timestamp
            if not validated_comment_dict['edited']:
                validated_comment_dict['edited'] = None
            
            comments.append(validated_comment_dict)

        except ValidationError as e:
            invalid_comment = {"invalid_record": comment.__dict__, "error": e.__str__()}
            logger.warning(f"Failed to validate comment: {invalid_comment} with exception {e}")
            invalid_comments.append(invalid_comment)
        
        except praw.exceptions.RedditAPIException as e:
            logger.error(f"Request failed while fetching comments with server exception {e}")
            raise e
        
        except praw.exceptions.ClientException as e:
            logger.error(f"Request failed while fetching comments with client exception {e}")
            raise e

    if comments:
        logger.info(f"Successfully fetched {len(comments)} comments from submission {submission.id}")
    else:
        logger.info(f"No comments fetched from submission {submission.id}")
    if invalid_comments:
        logger.warning(f"Failed to validate {len(invalid_comments)} comments from submission {submission.id}")

    return comments, invalid_comments

def fetch_reddit():
    execution_ts = datetime.now(UTC).timestamp()
    execution_date_utc = datetime.fromtimestamp(execution_ts, UTC).strftime('%Y-%m-%d')

    load_dotenv()
    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")

    s3_bucket = os.getenv("AWS_S3_BUCKET")
    s3_client = boto3.client('s3')

    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent="mad_dashboard_app",
    )

    logger = setup_s3_logging(
        logger_name='reddit_api',
        s3_bucket=s3_bucket,
        s3_log_dir='logs',
        s3_client=s3_client,
        local_log_dir=f'.dev/logs'
    )

    for sub in SUBREDDITS:
        sub = sub.lower()
        
        # Submissions        
        logger.info(f"Fetching newest submissions from r/{sub}")
        submissions, invalid_submissions = fetch_subreddit_newest_submissions(reddit, sub, logger, limit=100)
        valid_submissions_target_file = f"reddit/api/{execution_date_utc}/valid/{sub}_submissions.json"
        invalid_submissions_target_file = f"reddit/api/{execution_date_utc}/invalid/{sub}_submissions.json"

        if submissions:
            submissions_json = json.dumps(submissions)
            put_to_s3(
                data=submissions_json,
                s3_bucket=s3_bucket,
                s3_file=valid_submissions_target_file,
                s3_client=s3_client,
                logger=logger
            )
        if invalid_submissions:
            invalid_submissions_json = json.dumps(invalid_submissions)
            put_to_s3(
                data=invalid_submissions_json,
                s3_bucket=s3_bucket,
                s3_file=invalid_submissions_target_file,
                s3_client=s3_client,
                logger=logger
            )

        # Comments
        all_comments = []
        all_invalid_comments = []
        logger.info(f"Fetching comments for retrieved submissions from r/{sub}")
        for submission in submissions:
            comments, invalid_comments = fetch_submission_comments(reddit, submission['id'], logger)
            all_comments.extend(comments)
            all_invalid_comments.extend(invalid_comments)
        
        valid_comments_target_file = f"reddit/api/{execution_date_utc}/valid/{sub}_comments.json"
        invalid_comments_target_file = f"reddit/api/{execution_date_utc}/invalid/{sub}_comments.json"

        if all_comments:
            all_comments_json = json.dumps(all_comments)
            put_to_s3(
                data=all_comments_json,
                s3_bucket=s3_bucket,
                s3_file=valid_comments_target_file,
                s3_client=s3_client,
                logger=logger
            )
        if all_invalid_comments:
            all_invalid_comments_json = json.dumps(all_invalid_comments)
            put_to_s3(
                data=all_invalid_comments_json,
                s3_bucket=s3_bucket,
                s3_file=invalid_comments_target_file,
                s3_client=s3_client,
                logger=logger
            )
        
        # Sleep for 1 minute before fetching next subreddit
        time.sleep(60)

# def write_to_duckdb(
#     data: Union[pd.DataFrame, List[dict]],
#     table_name: str,
#     db_path: str,
#     overwrite: bool=False,
# ):
#     if not isinstance(data, pd.DataFrame):
#         data = pd.DataFrame(data)

#     with duckdb.connect(db_path) as con:
#         # Create table if not exists
#         con.execute(create_submissions_table_query)
#         con.execute(create_comments_table_query)

#         # Write data to table
#         if overwrite:
#             con.execute(f"TRUNCATE TABLE {table_name}")
#         if data.shape[0] > 0:
#             con.execute(f"INSERT INTO {table_name} SELECT * FROM data")
#         else:
#             print("No data to write")

if __name__ == "__main__":
    fetch_reddit()