import time
import json
import boto3 # type: ignore
from botocore.exceptions import NoCredentialsError, ClientError # type: ignore
import logging # type: ignore
import praw # type: ignore
from typing import List, Tuple, Union, Optional
from datetime import datetime, UTC
from pydantic import BaseModel, ValidationError

# Pydantic Schemas
class RedditSubmission(BaseModel):
    archived: bool
    author_fullname: Optional[Union[str, None]] = None
    author_flair_css_class: Union[str, None]
    author_flair_text: Union[str, None]
    category: Union[str, None]
    clicked: Union[bool, None]
    created: float
    created_utc: float
    distinguished: Union[str, None]
    domain: Union[str, None]
    downs: int
    edited: Union[bool, float]
    gilded: int
    gildings: Union[dict, None]
    hidden: Union[bool, None]
    hide_score: Union[bool, None]
    id: str
    is_created_from_ads_ui: Union[bool, None]
    is_meta: Union[bool, None]
    is_original_content: Union[bool, None]
    is_reddit_media_domain: Union[bool, None]
    is_self: bool
    is_video: bool
    link_flair_css_class: Union[str, None]
    link_flair_text: Union[str, None]
    locked: bool
    media: Union[dict, None]
    media_embed: Union[dict, None]
    media_only: bool
    name: str
    no_follow: Union[bool, None]
    num_comments: int
    num_crossposts: Union[int, None]
    over_18: Union[bool, None]
    permalink: str
    pinned: bool
    pwls: int
    quarantine: bool
    removed_by_category: Union[str, None]
    saved: Union[bool, None]
    score: int
    secure_media: Union[dict, None]
    selftext: str
    selftext_html: Union[str, None]
    send_replies: Union[bool, None]
    spoiler: Union[bool, None]
    stickied: bool
    subreddit_id: str
    subreddit_name_prefixed: str
    subreddit_subscribers: int
    subreddit_type: Union[str, None]
    thumbnail: Union[str, None]
    title: str
    total_awards_received: Union[int, None]
    treatment_tags: Union[list, None]
    ups: int
    upvote_ratio: float
    url: str
    user_reports: Union[list, None]
    wls: Union[int, None]

class RedditComment(BaseModel):
    archived: bool
    author_fullname: Optional[Union[str, None]] = None
    author_flair_css_class: Union[str, None]
    author_flair_text: Union[str, None]
    awarders: Union[list, None]
    body: str
    body_html: Union[str, None]
    collapsed: Union[bool, None]
    collapsed_reason: Union[str, None]
    collapsed_reason_code: Union[str, None]
    controversiality: Union[int, None]
    created_utc: float
    distinguished: Union[str, None]
    downs: int
    edited: Union[bool, float]
    gilded: int
    gildings: Union[dict, None]
    id: str
    is_submitter: Union[bool, None]
    link_id: str
    locked: Union[bool, None]
    name: Union[str, None]
    no_follow: Union[bool, None]
    parent_id: str
    permalink: str
    score: int
    score_hidden: Union[bool, None]
    send_replies: Union[bool, None]
    stickied: bool
    subreddit_id: str
    subreddit_name_prefixed: str
    subreddit_type: Union[str, None]
    total_awards_received: Union[int, None]
    treatment_tags: Union[list, None]
    ups: int

def put_to_s3(data, s3_bucket, s3_file, s3_client, logger):
    try:
        # A put_object will overwrite the file if it already exists
        s3_client.put_object(Bucket=s3_bucket, Key=s3_file, Body=data)
        logger.info(f"Write to S3 successful: {s3_bucket}/{s3_file}")
        return f's3://{s3_bucket}/{s3_file}'
    except NoCredentialsError as e:
        logger.error("Credentials not available")
        raise e
    except ClientError as e:
        logger.error(f"Client error: {e}")
        raise e

def fetch_subreddit_newest_submissions(
    reddit: praw.reddit.Reddit,
    subreddit_name: str,
    logger: logging.Logger,
    limit: int=100,
) -> Tuple[List[dict], List[dict]]:
    # Instantiate subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime('%Y-%m-%d')

    # Fetch newest submissions
    top_new = subreddit.new(limit=limit)

    # Initialize list for validated and invalid submissions
    submissions: List[dict] = []
    invalid_submissions: List[dict] = []

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
) -> Tuple[List[dict], List[dict]]:
    if isinstance(submission, str):
        submission = reddit.submission(submission)
    
    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime('%Y-%m-%d')

    # Retrieve comments recursively
    submission.comments.replace_more(limit=limit)

    # Initialize list for validated and invalid submissions
    comments: List[dict] = []
    invalid_comments: List[dict] = []

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

def fetch_reddit(
    subreddits: List[str],
    s3_bucket: str,
    reddit_client_id: str,
    reddit_client_secret: str,
    logger: logging.Logger
):
    execution_ts = datetime.now(UTC).timestamp()
    execution_date_utc = datetime.fromtimestamp(execution_ts, UTC).strftime('%Y-%m-%d')

    s3_client = boto3.client(
        's3'
    )

    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent="mad_dashboard_app"
    )

    for sub in subreddits:
        sub = sub.lower()
        
        # Submissions        
        logger.info(f"Fetching newest submissions from r/{sub}")
        submissions, invalid_submissions = fetch_subreddit_newest_submissions(reddit, sub, logger, limit=100)
        valid_submissions_target_file = f"data/reddit/api/{execution_date_utc}/valid/{sub}_submissions.json"
        invalid_submissions_target_file = f"data/reddit/api/{execution_date_utc}/invalid/{sub}_submissions.json"

        if submissions:
            submissions_json = "\n".join([json.dumps(record) for record in submissions])
            put_to_s3(
                data=submissions_json,
                s3_bucket=s3_bucket,
                s3_file=valid_submissions_target_file,
                s3_client=s3_client,
                logger=logger
            )
        if invalid_submissions:
            invalid_submissions_json = "\n".join([json.dumps(record) for record in invalid_submissions])
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
        
        valid_comments_target_file = f"data/reddit/api/{execution_date_utc}/valid/{sub}_comments.json"
        invalid_comments_target_file = f"data/reddit/api/{execution_date_utc}/invalid/{sub}_comments.json"

        if all_comments:
            all_comments_json = "\n".join([json.dumps(record) for record in all_comments])
            put_to_s3(
                data=all_comments_json,
                s3_bucket=s3_bucket,
                s3_file=valid_comments_target_file,
                s3_client=s3_client,
                logger=logger
            )
        if all_invalid_comments:
            all_invalid_comments_json = "\n".join([json.dumps(record) for record in all_invalid_comments])
            put_to_s3(
                data=all_invalid_comments_json,
                s3_bucket=s3_bucket,
                s3_file=invalid_comments_target_file,
                s3_client=s3_client,
                logger=logger
            )
        
        # Sleep for 1 minute before fetching next subreddit
        time.sleep(60)

def lambda_handler(payload, context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info(f"Starting lambda function {context.function_name} version {context.function_version}")
    logger.info(f"Lambda Request ID: {context.aws_request_id}")
    logger.info(f"Lambda function memory limits in MB: {context.memory_limit_in_mb}")
    
    try:
        fetch_reddit(
            subreddits=payload['subreddits'],
            s3_bucket=payload['s3_bucket'],
            reddit_client_id=payload['reddit_client_id'],
            reddit_client_secret=payload['reddit_client_secret'],
            logger=logger
        )
    except Exception as e:
        logger.error(f"Error fetching reddit data: {e}")
        raise e
    finally:
        logger.info(f"Lambda function {context.function_name} version {context.function_version} completed")
        return {
            'statusCode': 200,
            'body': 'Reddit data fetched successfully'
        }