import time
import json
import boto3  # type: ignore
from botocore.exceptions import NoCredentialsError, ClientError  # type: ignore
import logging  # type: ignore
import praw  # type: ignore
from typing import List, Tuple, Union, Optional, Dict, Any
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
        return f"s3://{s3_bucket}/{s3_file}"

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
    submission_limit: int = 100,
) -> Tuple[List[dict], List[dict]]:
    # Instantiate subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime("%Y-%m-%d")

    # Fetch newest submissions
    top_new = subreddit.new(limit=submission_limit)

    # Initialize list for validated and invalid submissions
    submissions: List[dict] = []
    invalid_submissions: List[dict] = []

    for submission in top_new:
        try:
            # Validate submission
            validated_submission = RedditSubmission(**submission.__dict__)
            validated_submission_dict = dict(validated_submission)

            # Add fetched timestamp and date
            validated_submission_dict["_fetched_date"] = _fetched_date
            validated_submission_dict["_fetched_iso_utc"] = _fetched_iso_utc

            # Convert edited to None if False as the value is not a timestamp
            if not validated_submission_dict["edited"]:
                validated_submission_dict["edited"] = None

            submissions.append(validated_submission_dict)

        except ValidationError as e:
            invalid_submission = {
                "invalid_record": submission.__dict__,
                "error": e.__str__(),
            }
            logger.warning(
                f"Failed to validate submission: {invalid_submission} with exception {e}"
            )
            invalid_submissions.append(invalid_submission)

        except praw.exceptions.RedditAPIException as e:
            logger.error(
                f"Request failed while fetching submission with server exception {e}"
            )
            raise e

        except praw.exceptions.ClientException as e:
            logger.error(
                f"Request failed while fetching submission with client exception {e}"
            )
            raise e

    logger.info(
        f"Successfully fetched {len(submissions)} submissions from r/{subreddit_name}"
    )
    if invalid_submissions:
        logger.warning(
            f"Failed to validate {len(invalid_submissions)} submissions from r/{subreddit_name}"
        )

    return submissions, invalid_submissions


def fetch_submission_comments(
    reddit: praw.reddit.Reddit,
    submission: Union[praw.models.Submission, str],
    logger: logging.Logger,
    comment_replace_more_limit: Union[int, None] = None,
) -> Tuple[List[dict], List[dict]]:
    if isinstance(submission, str):
        submission = reddit.submission(submission)

    # Timestamp of fetching
    _fetched_ts = datetime.now(UTC).timestamp()
    _fetched_iso_utc = datetime.fromtimestamp(_fetched_ts, UTC).isoformat()
    _fetched_date = datetime.fromtimestamp(_fetched_ts, UTC).date().strftime("%Y-%m-%d")

    # Retrieve comments recursively
    submission.comments.replace_more(limit=comment_replace_more_limit)

    # Initialize list for validated and invalid submissions
    comments: List[dict] = []
    invalid_comments: List[dict] = []

    for comment in submission.comments.list():
        try:
            # Validate comment
            validated_comment = RedditComment(**comment.__dict__)
            validated_comment_dict = dict(validated_comment)

            # Add fetched timestamp and date
            validated_comment_dict["_fetched_date"] = _fetched_date
            validated_comment_dict["_fetched_iso_utc"] = _fetched_iso_utc

            # Convert edited to None if False as the value is not a timestamp
            if not validated_comment_dict["edited"]:
                validated_comment_dict["edited"] = None

            comments.append(validated_comment_dict)

        except ValidationError as e:
            invalid_comment = {"invalid_record": comment.__dict__, "error": e.__str__()}
            logger.warning(
                f"Failed to validate comment: {invalid_comment} with exception {e}"
            )
            invalid_comments.append(invalid_comment)

        except praw.exceptions.RedditAPIException as e:
            logger.error(
                f"Request failed while fetching comments with server exception {e}"
            )
            raise e

        except praw.exceptions.ClientException as e:
            logger.error(
                f"Request failed while fetching comments with client exception {e}"
            )
            raise e

    if comments:
        logger.info(
            f"Successfully fetched {len(comments)} comments from submission {submission.id}"
        )
    else:
        logger.info(f"No comments fetched from submission {submission.id}")
    if invalid_comments:
        logger.warning(
            f"Failed to validate {len(invalid_comments)} comments from submission {submission.id}"
        )

    return comments, invalid_comments


def fetch_reddit(
    reddit_client: praw.Reddit,
    subreddit: str,
    logger: logging.Logger,
    submission_limit: int = 100,
    comment_replace_more_limit: Union[int, None] = None,
) -> Dict[str, Any]:
    """
    Fetches submissions and their comments for a given subreddit using the PRAW client.

    This function retrieves the newest submissions based on `submission_limit`.
    For each valid submission, it then fetches comments. The depth of comment
    fetching is controlled by `comment_replace_more_limit`, which corresponds to
    PRAW's `replace_more(limit=...)` parameter. If `comment_replace_more_limit`
    is None, PRAW attempts to fetch all comments. If it's an integer, PRAW
    makes at most that many calls to expand "load more comments" nodes.

    Args:
        reddit_client: An initialized PRAW Reddit instance.
        subreddit: The name of the subreddit to fetch data from (case-insensitive).
        logger: A logger instance for logging information and errors.
        submission_limit: The maximum number of newest submissions to fetch.
        comment_replace_more_limit: Controls the depth of comment fetching.
            Corresponds to PRAW's `replace_more(limit=...)`.
            `None` attempts to get all comments. An integer N makes at most N
            "load more" calls. `0` fetches only initially visible comments.

    Returns:
        A dictionary containing:
            'execution_date_utc': The UTC date string (YYYY-MM-DD) of when the fetch occurred.
            'valid_submissions': A list of successfully fetched and validated submission data.
            'invalid_submissions': A list of submissions that failed PRAW validation.
            'valid_comments': A list of successfully fetched and validated comment data.
            'invalid_comments': A list of comments that failed PRAW validation.
    """
    execution_date_utc = datetime.fromtimestamp(datetime.now(UTC).timestamp()).strftime(
        "%Y-%m-%d"
    )
    sub = subreddit  # Already lowercased by handler

    logger.info(f"Fetching newest submissions from r/{sub}")
    valid_submissions, invalid_submissions = fetch_subreddit_newest_submissions(
        reddit_client, sub, logger, submission_limit=submission_limit
    )

    all_valid_comments = []
    all_invalid_comments = []
    if valid_submissions:  # Only fetch comments if there are submissions
        logger.info(
            f"Fetching comments for {len(valid_submissions)} retrieved submissions from r/{sub}"
        )
        for submission_obj in valid_submissions:
            comments, invalid_comments_for_submission = fetch_submission_comments(
                reddit_client,
                submission_obj["id"],
                logger,
                comment_replace_more_limit=comment_replace_more_limit,
            )
            all_valid_comments.extend(comments)
            all_invalid_comments.extend(invalid_comments_for_submission)

    return {
        "execution_date_utc": execution_date_utc,
        "valid_submissions": valid_submissions,
        "invalid_submissions": invalid_submissions,
        "valid_comments": all_valid_comments,
        "invalid_comments": all_invalid_comments,
    }


def lambda_handler(payload, context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info(
        f"Starting lambda function {context.function_name} version {context.function_version}"
    )
    logger.info(f"Lambda Request ID: {context.aws_request_id}")
    logger.info(f"Lambda function memory limits in MB: {context.memory_limit_in_mb}")

    try:
        reddit_client_id = payload["reddit_client_id"]
        reddit_client_secret = payload["reddit_client_secret"]
        tasks = payload.get("tasks", [])

        if not isinstance(tasks, list):
            logger.error("Payload 'tasks' must be a list.")
            raise TypeError("Payload 'tasks' must be a list.")

        if not tasks:
            logger.warning("No tasks found in payload. Exiting.")
            return

        # Initialize PRAW client once
        reddit_client = praw.Reddit(
            client_id=reddit_client_id,
            client_secret=reddit_client_secret,
            user_agent="mad_dashboard_app",
        )

        # Initialize S3 client once
        s3_client = boto3.client("s3")

        for i, task in enumerate(tasks):
            subreddit_name = task.get("subreddit")
            s3_bucket_name = task.get("s3_bucket")
            submission_lim = task.get("submission_limit", 100)
            comment_replace_more_lim = task.get("comment_replace_more_limit")

            logger.info(
                f"Processing task {i + 1}/{len(tasks)} for subreddit: {subreddit_name}"
            )

            if not subreddit_name or not s3_bucket_name:
                logger.error(
                    f"Task {i + 1} is missing 'subreddit' or 's3_bucket'. Skipping: {task}"
                )
                continue

            sub_name_lower = subreddit_name.lower()

            fetched_data = fetch_reddit(
                reddit_client=reddit_client,
                subreddit=sub_name_lower,
                logger=logger,
                submission_limit=submission_lim,
                comment_replace_more_limit=comment_replace_more_lim,
            )

            exec_date = fetched_data["execution_date_utc"]

            # Store valid submissions
            if fetched_data["valid_submissions"]:
                submissions_json = "\n".join(
                    [json.dumps(record) for record in fetched_data["valid_submissions"]]
                )
                put_to_s3(
                    data=submissions_json,
                    s3_bucket=s3_bucket_name,
                    s3_file=f"data/reddit/api/{exec_date}/valid/{sub_name_lower}_submissions.json",
                    s3_client=s3_client,
                    logger=logger,
                )
            # Store invalid submissions
            if fetched_data["invalid_submissions"]:
                invalid_submissions_json = "\n".join(
                    [
                        json.dumps(record)
                        for record in fetched_data["invalid_submissions"]
                    ]
                )
                put_to_s3(
                    data=invalid_submissions_json,
                    s3_bucket=s3_bucket_name,
                    s3_file=f"data/reddit/api/{exec_date}/invalid/{sub_name_lower}_submissions.json",
                    s3_client=s3_client,
                    logger=logger,
                )
            # Store valid comments
            if fetched_data["valid_comments"]:
                comments_json = "\n".join(
                    [json.dumps(record) for record in fetched_data["valid_comments"]]
                )
                put_to_s3(
                    data=comments_json,
                    s3_bucket=s3_bucket_name,
                    s3_file=f"data/reddit/api/{exec_date}/valid/{sub_name_lower}_comments.json",
                    s3_client=s3_client,
                    logger=logger,
                )
            # Store invalid comments
            if fetched_data["invalid_comments"]:
                invalid_comments_json = "\n".join(
                    [json.dumps(record) for record in fetched_data["invalid_comments"]]
                )
                put_to_s3(
                    data=invalid_comments_json,
                    s3_bucket=s3_bucket_name,
                    s3_file=f"data/reddit/api/{exec_date}/invalid/{sub_name_lower}_comments.json",
                    s3_client=s3_client,
                    logger=logger,
                )

            if i < len(tasks) - 1:
                logger.info(f"Sleeping for 60 seconds before next task...")
                time.sleep(60)

        logger.info(
            f"Lambda function {context.function_name} version {context.function_version} completed successfully."
        )
    except KeyError as e:
        logger.error(
            f"Missing a required top-level key in payload (e.g., reddit_client_id, reddit_client_secret, or tasks): {e}"
        )
        raise e
    except Exception as e:
        logger.error(f"Error processing Reddit tasks: {e}")
        raise e
