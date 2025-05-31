import json
import logging
from datetime import datetime, UTC
from unittest.mock import MagicMock, patch

import pytest  # type: ignore
import praw  # type: ignore
from botocore.exceptions import NoCredentialsError, ClientError  # type: ignore
from pydantic import ValidationError

from ..ingest_reddit import (
    put_to_s3,
    RedditSubmission,
    RedditComment,
    fetch_subreddit_newest_submissions,
    fetch_submission_comments,
    fetch_reddit,
    lambda_handler,
)


# Add the mock_logger fixture here
@pytest.fixture
def mock_logger():
    """Fixture to provide a MagicMock for logging.Logger."""
    return MagicMock(spec=logging.Logger)


# Configure a basic logger for tests if needed, or mock it
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_put_to_s3_success(mocker, mock_logger):
    mock_s3_instance = MagicMock()
    mock_boto3_client = mocker.patch("ingest_reddit.ingest_reddit.boto3.client")
    mock_boto3_client.return_value = mock_s3_instance

    data = json.dumps({"key": "value"})
    s3_bucket = "test-bucket"
    s3_file = "test-file.json"

    expected_s3_path = f"s3://{s3_bucket}/{s3_file}"

    result = put_to_s3(data, s3_bucket, s3_file, mock_s3_instance, mock_logger)

    mock_s3_instance.put_object.assert_called_once_with(
        Bucket=s3_bucket, Key=s3_file, Body=data
    )
    mock_logger.info.assert_called_once_with(
        f"Write to S3 successful: {s3_bucket}/{s3_file}"
    )
    assert result == expected_s3_path


def test_put_to_s3_no_credentials_error(mocker, mock_logger):
    mock_s3_instance = MagicMock()
    mock_boto3_client = mocker.patch("ingest_reddit.ingest_reddit.boto3.client")
    mock_boto3_client.return_value = mock_s3_instance
    mock_s3_instance.put_object.side_effect = NoCredentialsError()

    data = json.dumps({"key": "value"})
    s3_bucket = "test-bucket"
    s3_file = "test-file.json"

    with pytest.raises(NoCredentialsError):
        put_to_s3(data, s3_bucket, s3_file, mock_s3_instance, mock_logger)

    mock_s3_instance.put_object.assert_called_once_with(
        Bucket=s3_bucket, Key=s3_file, Body=data
    )
    mock_logger.error.assert_called_once_with("Credentials not available")


def test_put_to_s3_client_error(mocker, mock_logger):
    mock_s3_instance = MagicMock()
    mock_boto3_client = mocker.patch("ingest_reddit.ingest_reddit.boto3.client")
    mock_boto3_client.return_value = mock_s3_instance
    client_error = ClientError(
        {"Error": {"Code": "SomeError", "Message": "Details"}}, "put_object"
    )
    mock_s3_instance.put_object.side_effect = client_error

    data = json.dumps({"key": "value"})
    s3_bucket = "test-bucket"
    s3_file = "test-file.json"

    with pytest.raises(ClientError):
        put_to_s3(data, s3_bucket, s3_file, mock_s3_instance, mock_logger)

    mock_s3_instance.put_object.assert_called_once_with(
        Bucket=s3_bucket, Key=s3_file, Body=data
    )
    mock_logger.error.assert_called_once_with(f"Client error: {client_error}")


def test_reddit_submission_validation_valid():
    valid_data = {
        "archived": False,
        "author_fullname": "t2_123",
        "author_flair_css_class": None,
        "author_flair_text": None,
        "category": None,
        "clicked": False,
        "created": 1678886400.0,
        "created_utc": 1678886400.0,
        "distinguished": None,
        "domain": "self.python",
        "downs": 0,
        "edited": False,
        "gilded": 0,
        "gildings": {},
        "hidden": False,
        "hide_score": False,
        "id": "t3_abc123",
        "is_created_from_ads_ui": False,
        "is_meta": False,
        "is_original_content": False,
        "is_reddit_media_domain": False,
        "is_self": True,
        "is_video": False,
        "link_flair_css_class": None,
        "link_flair_text": None,
        "locked": False,
        "media": None,
        "media_embed": {},
        "media_only": False,
        "name": "t3_abc123",
        "no_follow": True,
        "num_comments": 10,
        "num_crossposts": 0,
        "over_18": False,
        "permalink": "/r/python/comments/abc123/test_submission/",
        "pinned": False,
        "pwls": 6,
        "quarantine": False,
        "removed_by_category": None,
        "saved": False,
        "score": 100,
        "secure_media": None,
        "selftext": "This is a test submission.",
        "selftext_html": "<p>This is a test submission.</p>",
        "send_replies": True,
        "spoiler": False,
        "stickied": False,
        "subreddit_id": "t5_2qh0u",
        "subreddit_name_prefixed": "r/python",
        "subreddit_subscribers": 1000000,
        "subreddit_type": "public",
        "thumbnail": "self",
        "title": "Test Submission",
        "total_awards_received": 0,
        "treatment_tags": [],
        "ups": 100,
        "upvote_ratio": 0.9,
        "url": "https://www.reddit.com/r/python/comments/abc123/test_submission/",
        "user_reports": [],
        "wls": 6,
    }
    try:
        RedditSubmission(**valid_data)
    except ValidationError as e:
        pytest.fail(f"RedditSubmission validation failed for valid data: {e}")


def test_reddit_submission_validation_invalid():
    invalid_data = {"id": "abc123"}  # Missing many required fields
    with pytest.raises(ValidationError):
        RedditSubmission(**invalid_data)


def test_reddit_comment_validation_valid():
    valid_data = {
        "archived": False,
        "author_fullname": "t2_123",
        "author_flair_css_class": None,
        "author_flair_text": None,
        "awarders": [],
        "body": "This is a test comment.",
        "body_html": "<p>This is a test comment.</p>",
        "collapsed": False,
        "collapsed_reason": None,
        "collapsed_reason_code": None,
        "controversiality": 0,
        "created_utc": 1678886400.0,
        "distinguished": None,
        "downs": 0,
        "edited": False,
        "gilded": 0,
        "gildings": {},
        "id": "t1_def456",
        "is_submitter": False,
        "link_id": "t3_abc123",
        "locked": False,
        "name": "t1_def456",
        "no_follow": True,
        "parent_id": "t3_abc123",
        "permalink": "/r/python/comments/abc123/test_submission/def456/",
        "score": 10,
        "score_hidden": False,
        "send_replies": True,
        "stickied": False,
        "subreddit_id": "t5_2qh0u",
        "subreddit_name_prefixed": "r/python",
        "subreddit_type": "public",
        "total_awards_received": 0,
        "treatment_tags": [],
        "ups": 10,
    }
    try:
        RedditComment(**valid_data)
    except ValidationError as e:
        pytest.fail(f"RedditComment validation failed for valid data: {e}")


def test_reddit_comment_validation_invalid():
    invalid_data = {"id": "def456"}  # Missing many required fields
    with pytest.raises(ValidationError):
        RedditComment(**invalid_data)


@patch("ingest_reddit.ingest_reddit.praw.reddit.Reddit")
@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_subreddit_newest_submissions_success(
    mock_datetime_mod, mock_praw_reddit_class, mock_logger
):
    mock_reddit_instance = MagicMock()
    mock_praw_reddit_class.return_value = mock_reddit_instance
    mock_subreddit = MagicMock()
    mock_reddit_instance.subreddit.return_value = mock_subreddit

    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_now
    mock_datetime_mod.fromtimestamp.side_effect = lambda ts, tz: datetime.fromtimestamp(
        ts, tz
    )

    praw_submission_1_dict = {
        "archived": False,
        "author_fullname": "t2_user1",
        "author_flair_css_class": None,
        "author_flair_text": None,
        "category": None,
        "clicked": False,
        "created": 1672531200.0,
        "created_utc": 1672531200.0,
        "distinguished": None,
        "domain": "self.test",
        "downs": 0,
        "edited": False,
        "gilded": 0,
        "gildings": {},
        "hidden": False,
        "hide_score": False,
        "id": "sub1",
        "is_created_from_ads_ui": False,
        "is_meta": False,
        "is_original_content": False,
        "is_reddit_media_domain": False,
        "is_self": True,
        "is_video": False,
        "link_flair_css_class": None,
        "link_flair_text": None,
        "locked": False,
        "media": None,
        "media_embed": {},
        "media_only": False,
        "name": "t3_sub1",
        "no_follow": True,
        "num_comments": 5,
        "num_crossposts": 0,
        "over_18": False,
        "permalink": "/r/test/comments/sub1/",
        "pinned": False,
        "pwls": 6,
        "quarantine": False,
        "removed_by_category": None,
        "saved": False,
        "score": 10,
        "secure_media": None,
        "selftext": "text1",
        "selftext_html": "<p>text1</p>",
        "send_replies": True,
        "spoiler": False,
        "stickied": False,
        "subreddit_id": "t5_test",
        "subreddit_name_prefixed": "r/test",
        "subreddit_subscribers": 100,
        "subreddit_type": "public",
        "thumbnail": "self",
        "title": "title1",
        "total_awards_received": 0,
        "treatment_tags": [],
        "ups": 10,
        "upvote_ratio": 1.0,
        "url": "url1",
        "user_reports": [],
        "wls": 6,
    }
    mock_submission_1 = MagicMock(spec=praw.models.Submission)
    mock_submission_1.__dict__ = praw_submission_1_dict

    mock_subreddit.new.return_value = [mock_submission_1]

    submissions, invalid_submissions = fetch_subreddit_newest_submissions(
        mock_reddit_instance, "test_subreddit", mock_logger, submission_limit=1
    )

    assert len(submissions) == 1
    assert len(invalid_submissions) == 0
    assert submissions[0]["id"] == "sub1"
    assert submissions[0]["_fetched_date"] == "2023-01-01"
    assert submissions[0]["_fetched_iso_utc"] == "2023-01-01T12:00:00+00:00"
    assert submissions[0]["edited"] is None

    mock_reddit_instance.subreddit.assert_called_once_with("test_subreddit")
    mock_subreddit.new.assert_called_once_with(limit=1)
    mock_logger.info.assert_any_call(
        "Successfully fetched 1 submissions from r/test_subreddit"
    )


@patch("ingest_reddit.ingest_reddit.praw.reddit.Reddit")
@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_subreddit_newest_submissions_validation_error(
    mock_datetime_mod, mock_praw_reddit_class, mock_logger
):
    mock_reddit_instance = MagicMock()
    mock_praw_reddit_class.return_value = mock_reddit_instance
    mock_subreddit = MagicMock()
    mock_reddit_instance.subreddit.return_value = mock_subreddit

    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_now

    invalid_praw_submission_dict = {"id": "sub_invalid"}
    mock_invalid_submission = MagicMock(spec=praw.models.Submission)
    mock_invalid_submission.__dict__ = invalid_praw_submission_dict

    mock_subreddit.new.return_value = [mock_invalid_submission]

    submissions, invalid_submissions = fetch_subreddit_newest_submissions(
        mock_reddit_instance, "test_subreddit", mock_logger, submission_limit=1
    )

    assert len(submissions) == 0
    assert len(invalid_submissions) == 1
    assert invalid_submissions[0]["invalid_record"]["id"] == "sub_invalid"
    assert "error" in invalid_submissions[0]
    args, kwargs = mock_logger.warning.call_args_list[0]
    assert "Failed to validate submission:" in args[0]
    assert "sub_invalid" in args[0]
    assert (
        mock_logger.warning.call_args_list[1][0][0]
        == "Failed to validate 1 submissions from r/test_subreddit"
    )


@patch("ingest_reddit.ingest_reddit.praw.reddit.Reddit")
def test_fetch_subreddit_newest_submissions_praw_api_exception(
    mock_praw_reddit_class, mock_logger
):
    mock_reddit_instance = MagicMock()
    mock_praw_reddit_class.return_value = mock_reddit_instance
    mock_subreddit = MagicMock()
    mock_reddit_instance.subreddit.return_value = mock_subreddit
    mock_subreddit.new.side_effect = praw.exceptions.RedditAPIException(
        [["test_error_type", "Test API Exception", ""]], "Test API Exception", None
    )

    with pytest.raises(praw.exceptions.RedditAPIException):
        fetch_subreddit_newest_submissions(
            mock_reddit_instance, "test_subreddit", mock_logger
        )

    # mock_logger.error.assert_called_once() # This won't be called if the exception is in subreddit.new()


@patch("ingest_reddit.ingest_reddit.praw.reddit.Reddit")
def test_fetch_subreddit_newest_submissions_praw_client_exception(
    mock_praw_reddit_class, mock_logger
):
    mock_reddit_instance = MagicMock()
    mock_praw_reddit_class.return_value = mock_reddit_instance
    mock_subreddit = MagicMock()
    mock_reddit_instance.subreddit.return_value = mock_subreddit
    mock_subreddit.new.side_effect = praw.exceptions.ClientException(
        "Test Client Exception"
    )

    with pytest.raises(praw.exceptions.ClientException):
        fetch_subreddit_newest_submissions(
            mock_reddit_instance, "test_subreddit", mock_logger
        )

    # mock_logger.error.assert_called_once() # This won't be called if the exception is in subreddit.new()


@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_submission_comments_success(mock_datetime_mod, mocker, mock_logger):
    mock_reddit_instance = MagicMock(spec=praw.Reddit)

    mock_submission_obj = MagicMock(spec=praw.models.Submission)
    mock_submission_obj.id = "test_sub_id"
    mock_submission_obj.comments = MagicMock()

    mock_now = datetime(2023, 1, 2, 10, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_now
    mock_datetime_mod.fromtimestamp.side_effect = lambda ts, tz: datetime.fromtimestamp(
        ts, tz
    )

    praw_comment_1_dict = {
        "archived": False,
        "author_fullname": "t2_user2",
        "author_flair_css_class": None,
        "author_flair_text": None,
        "awarders": [],
        "body": "Comment body 1",
        "body_html": "<p>Comment body 1</p>",
        "collapsed": False,
        "collapsed_reason": None,
        "collapsed_reason_code": None,
        "controversiality": 0,
        "created_utc": 1672653600.0,
        "distinguished": None,
        "downs": 0,
        "edited": False,
        "gilded": 0,
        "gildings": {},
        "id": "com1",
        "is_submitter": False,
        "link_id": "t3_test_sub_id",
        "locked": False,
        "name": "t1_com1",
        "no_follow": True,
        "parent_id": "t3_test_sub_id",
        "permalink": "/r/test/comments/test_sub_id/_/com1/",
        "score": 5,
        "score_hidden": False,
        "send_replies": True,
        "stickied": False,
        "subreddit_id": "t5_test",
        "subreddit_name_prefixed": "r/test",
        "subreddit_type": "public",
        "total_awards_received": 0,
        "treatment_tags": [],
        "ups": 5,
    }
    mock_comment_1 = MagicMock(spec=praw.models.Comment)
    mock_comment_1.__dict__ = praw_comment_1_dict

    mock_submission_obj.comments.list.return_value = [mock_comment_1]
    mock_reddit_instance.submission.return_value = mock_submission_obj

    comments, invalid_comments = fetch_submission_comments(
        mock_reddit_instance, "test_sub_id", mock_logger, comment_replace_more_limit=0
    )

    assert len(comments) == 1
    assert len(invalid_comments) == 0
    assert comments[0]["id"] == "com1"
    assert comments[0]["_fetched_date"] == "2023-01-02"
    assert comments[0]["_fetched_iso_utc"] == "2023-01-02T10:00:00+00:00"
    assert comments[0]["edited"] is None

    mock_submission_obj.comments.replace_more.assert_called_once_with(limit=0)
    mock_reddit_instance.submission.assert_called_once_with("test_sub_id")
    mock_logger.info.assert_any_call(
        "Successfully fetched 1 comments from submission test_sub_id"
    )

    mock_reddit_instance.submission.reset_mock()
    comments_obj_pass, _ = fetch_submission_comments(
        mock_reddit_instance,
        mock_submission_obj,
        mock_logger,
        comment_replace_more_limit=None,
    )
    assert len(comments_obj_pass) == 1
    mock_submission_obj.comments.replace_more.assert_called_with(limit=None)
    mock_reddit_instance.submission.assert_not_called()


@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_submission_comments_validation_error(
    mock_datetime_mod, mocker, mock_logger
):
    mock_reddit_instance = MagicMock(spec=praw.Reddit)
    mock_submission_obj = MagicMock(spec=praw.models.Submission)
    mock_submission_obj.id = "test_sub_id_invalid"
    mock_submission_obj.comments = MagicMock()

    mock_now = datetime(2023, 1, 2, 10, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_now

    invalid_praw_comment_dict = {"id": "com_invalid"}
    mock_invalid_comment = MagicMock(spec=praw.models.Comment)
    mock_invalid_comment.__dict__ = invalid_praw_comment_dict

    mock_submission_obj.comments.list.return_value = [mock_invalid_comment]
    mock_reddit_instance.submission.return_value = mock_submission_obj

    comments, invalid_comments = fetch_submission_comments(
        mock_reddit_instance, "test_sub_id_invalid", mock_logger
    )

    assert len(comments) == 0
    assert len(invalid_comments) == 1
    assert invalid_comments[0]["invalid_record"]["id"] == "com_invalid"
    args, kwargs = mock_logger.warning.call_args_list[0]
    assert "Failed to validate comment:" in args[0]
    assert "com_invalid" in args[0]
    assert (
        mock_logger.warning.call_args_list[1][0][0]
        == "Failed to validate 1 comments from submission test_sub_id_invalid"
    )


def test_fetch_submission_comments_praw_api_exception(mocker, mock_logger):
    mock_reddit_instance = MagicMock(spec=praw.Reddit)
    mock_submission_obj = MagicMock(spec=praw.models.Submission)
    mock_submission_obj.id = "test_sub_id_api_ex"
    mock_submission_obj.comments = MagicMock()
    mock_submission_obj.comments.replace_more.side_effect = (
        praw.exceptions.RedditAPIException(
            [["test_error_type", "Test API Exception", ""]], "Test API Exception", None
        )
    )
    mock_reddit_instance.submission.return_value = mock_submission_obj

    with pytest.raises(praw.exceptions.RedditAPIException):
        fetch_submission_comments(
            mock_reddit_instance, "test_sub_id_api_ex", mock_logger
        )


def test_fetch_submission_comments_praw_client_exception(mocker, mock_logger):
    mock_reddit_instance = MagicMock(spec=praw.Reddit)
    mock_submission_obj = MagicMock(spec=praw.models.Submission)
    mock_submission_obj.id = "test_sub_id_client_ex"
    mock_submission_obj.comments = MagicMock()
    mock_submission_obj.comments.replace_more.side_effect = (
        praw.exceptions.ClientException("Test Client Exception")
    )
    mock_reddit_instance.submission.return_value = mock_submission_obj

    with pytest.raises(praw.exceptions.ClientException):
        fetch_submission_comments(
            mock_reddit_instance, "test_sub_id_client_ex", mock_logger
        )


@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_submission_comments_no_comments(mock_datetime_mod, mocker, mock_logger):
    mock_reddit_instance = MagicMock(spec=praw.Reddit)
    mock_submission_obj = MagicMock(spec=praw.models.Submission)
    mock_submission_obj.id = "test_sub_no_comments"
    mock_submission_obj.comments = MagicMock()
    mock_submission_obj.comments.list.return_value = []
    mock_reddit_instance.submission.return_value = mock_submission_obj

    mock_now = datetime(2023, 1, 2, 10, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_now
    mock_datetime_mod.fromtimestamp.side_effect = lambda ts, tz: datetime.fromtimestamp(
        ts, tz
    )

    comments, invalid_comments = fetch_submission_comments(
        mock_reddit_instance, "test_sub_no_comments", mock_logger
    )

    assert len(comments) == 0
    assert len(invalid_comments) == 0
    mock_submission_obj.comments.replace_more.assert_called_once()
    mock_logger.info.assert_any_call(
        "No comments fetched from submission test_sub_no_comments"
    )


@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_reddit_success(mock_datetime_mod, mocker, mock_logger):
    mock_praw_client = MagicMock(spec=praw.Reddit)
    subreddit_name = "datascience"
    submission_limit = 50
    comment_limit = 10

    mock_exec_time = datetime(2023, 10, 26, 15, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_exec_time
    mock_datetime_mod.fromtimestamp.side_effect = (
        lambda ts, tz=None: datetime.fromtimestamp(ts, tz if tz else UTC)
    )

    valid_submission_1 = {"id": "sub1", "title": "Submission 1"}
    mock_fetch_submissions = mocker.patch(
        "ingest_reddit.ingest_reddit.fetch_subreddit_newest_submissions"
    )
    mock_fetch_submissions.return_value = (
        [valid_submission_1],
        [{"id": "invalid_sub1"}],
    )

    valid_comment_1 = {"id": "com1", "body": "Comment 1"}
    mock_fetch_comments = mocker.patch(
        "ingest_reddit.ingest_reddit.fetch_submission_comments"
    )
    mock_fetch_comments.return_value = ([valid_comment_1], [{"id": "invalid_com1"}])

    result = fetch_reddit(
        reddit_client=mock_praw_client,
        subreddit=subreddit_name,
        logger=mock_logger,
        submission_limit=submission_limit,
        comment_replace_more_limit=comment_limit,
    )

    expected_exec_date = "2023-10-26"
    assert result["execution_date_utc"] == expected_exec_date
    assert len(result["valid_submissions"]) == 1
    assert result["valid_submissions"][0]["id"] == "sub1"
    assert len(result["invalid_submissions"]) == 1
    assert len(result["valid_comments"]) == 1
    assert result["valid_comments"][0]["id"] == "com1"
    assert len(result["invalid_comments"]) == 1

    mock_fetch_submissions.assert_called_once_with(
        mock_praw_client, subreddit_name, mock_logger, submission_limit=submission_limit
    )
    mock_fetch_comments.assert_called_once_with(
        mock_praw_client,
        valid_submission_1["id"],
        mock_logger,
        comment_replace_more_limit=comment_limit,
    )
    mock_logger.info.assert_any_call(
        f"Fetching newest submissions from r/{subreddit_name}"
    )
    mock_logger.info.assert_any_call(
        f"Fetching comments for 1 retrieved submissions from r/{subreddit_name}"
    )


@patch("ingest_reddit.ingest_reddit.datetime")
def test_fetch_reddit_no_submissions(mock_datetime_mod, mocker, mock_logger):
    mock_praw_client = MagicMock(spec=praw.Reddit)
    subreddit_name = "nosubs"

    mock_exec_time = datetime(2023, 10, 26, 15, 0, 0, tzinfo=UTC)
    mock_datetime_mod.now.return_value = mock_exec_time
    mock_datetime_mod.fromtimestamp.side_effect = (
        lambda ts, tz=None: datetime.fromtimestamp(ts, tz if tz else UTC)
    )

    mock_fetch_submissions = mocker.patch(
        "ingest_reddit.ingest_reddit.fetch_subreddit_newest_submissions"
    )
    mock_fetch_submissions.return_value = ([], [])

    mock_fetch_comments = mocker.patch(
        "ingest_reddit.ingest_reddit.fetch_submission_comments"
    )

    result = fetch_reddit(
        reddit_client=mock_praw_client, subreddit=subreddit_name, logger=mock_logger
    )
    mock_fetch_comments.assert_not_called()

    assert result["execution_date_utc"] == "2023-10-26"
    assert len(result["valid_submissions"]) == 0
    assert len(result["invalid_submissions"]) == 0
    assert len(result["valid_comments"]) == 0
    assert len(result["invalid_comments"]) == 0
    mock_logger.info.assert_any_call(
        f"Fetching newest submissions from r/{subreddit_name}"
    )


def test_lambda_handler_success_single_task(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    mock_praw_instance = MagicMock()
    mock_praw_reddit_class = mocker.patch("ingest_reddit.ingest_reddit.praw.Reddit")
    mock_praw_reddit_class.return_value = mock_praw_instance
    mock_s3_instance = MagicMock()
    mock_boto3_client = mocker.patch("ingest_reddit.ingest_reddit.boto3.client")
    mock_boto3_client.return_value = mock_s3_instance
    mock_fetch_reddit_func = mocker.patch("ingest_reddit.ingest_reddit.fetch_reddit")
    mock_put_s3_func = mocker.patch("ingest_reddit.ingest_reddit.put_to_s3")
    mock_sleep = mocker.patch("ingest_reddit.ingest_reddit.time.sleep")

    payload = {
        "reddit_client_id": "test_id",
        "reddit_client_secret": "test_secret",
        "tasks": [
            {
                "subreddit": "python",
                "s3_bucket": "mybucket",
                "submission_limit": 10,
                "comment_replace_more_limit": 5,
            }
        ],
    }
    mock_context = MagicMock()
    mock_context.function_name = "test_lambda"
    mock_context.function_version = "1"
    mock_context.aws_request_id = "test_req_id"
    mock_context.memory_limit_in_mb = "128"

    fetch_reddit_response = {
        "execution_date_utc": "2023-01-01",
        "valid_submissions": [{"id": "s1"}],
        "invalid_submissions": [],
        "valid_comments": [{"id": "c1"}],
        "invalid_comments": [],
    }
    mock_fetch_reddit_func.return_value = fetch_reddit_response

    lambda_handler(payload, mock_context)

    mock_praw_reddit_class.assert_called_once_with(
        client_id="test_id", client_secret="test_secret", user_agent="mad_dashboard_app"
    )
    mock_boto3_client.assert_called_once_with("s3")
    mock_fetch_reddit_func.assert_called_once_with(
        reddit_client=mock_praw_instance,
        subreddit="python",
        logger=mock_logger,
        submission_limit=10,
        comment_replace_more_limit=5,
    )
    assert mock_put_s3_func.call_count == 2
    mock_put_s3_func.assert_any_call(
        data='{"id": "s1"}',
        s3_bucket="mybucket",
        s3_file="data/reddit/api/2023-01-01/valid/python_submissions.json",
        s3_client=mock_s3_instance,
        logger=mock_logger,
    )
    mock_put_s3_func.assert_any_call(
        data='{"id": "c1"}',
        s3_bucket="mybucket",
        s3_file="data/reddit/api/2023-01-01/valid/python_comments.json",
        s3_client=mock_s3_instance,
        logger=mock_logger,
    )

    mock_sleep.assert_not_called()
    mock_logger.info.assert_any_call("Processing task 1/1 for subreddit: python")
    mock_logger.info.assert_any_call(
        f"Lambda function {mock_context.function_name} version {mock_context.function_version} completed successfully."
    )


def test_lambda_handler_multiple_tasks_with_sleep(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    mock_praw_instance = MagicMock()
    mock_praw_reddit_class = mocker.patch("ingest_reddit.ingest_reddit.praw.Reddit")
    mock_praw_reddit_class.return_value = mock_praw_instance
    mock_s3_instance = MagicMock()
    mock_boto3_client = mocker.patch("ingest_reddit.ingest_reddit.boto3.client")
    mock_boto3_client.return_value = mock_s3_instance
    mock_fetch_reddit_func = mocker.patch("ingest_reddit.ingest_reddit.fetch_reddit")
    mock_put_s3_func = mocker.patch("ingest_reddit.ingest_reddit.put_to_s3")
    mock_sleep = mocker.patch("ingest_reddit.ingest_reddit.time.sleep")

    payload = {
        "reddit_client_id": "test_id",
        "reddit_client_secret": "test_secret",
        "tasks": [
            {"subreddit": "learnpython", "s3_bucket": "bucket1"},
            {"subreddit": "datascience", "s3_bucket": "bucket2"},
        ],
    }
    mock_context = MagicMock(
        function_name="test_lambda",
        function_version="1",
        aws_request_id="req2",
        memory_limit_in_mb="128",
    )

    fetch_reddit_response_1 = {
        "execution_date_utc": "2023-01-01",
        "valid_submissions": [{"id": "s1"}],
        "invalid_submissions": [],
        "valid_comments": [],
        "invalid_comments": [],
    }
    fetch_reddit_response_2 = {
        "execution_date_utc": "2023-01-01",
        "valid_submissions": [{"id": "s2"}],
        "invalid_submissions": [],
        "valid_comments": [],
        "invalid_comments": [],
    }
    mock_fetch_reddit_func.side_effect = [
        fetch_reddit_response_1,
        fetch_reddit_response_2,
    ]

    lambda_handler(payload, mock_context)

    assert mock_fetch_reddit_func.call_count == 2
    assert mock_put_s3_func.call_count == 2
    mock_sleep.assert_called_once_with(60)
    mock_logger.info.assert_any_call("Sleeping for 60 seconds before next task...")


def test_lambda_handler_missing_credentials(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    payload = {"tasks": [{"subreddit": "test", "s3_bucket": "b"}]}
    mock_context = MagicMock(
        function_name="f",
        function_version="v",
        aws_request_id="r",
        memory_limit_in_mb="m",
    )
    with pytest.raises(KeyError) as excinfo:
        lambda_handler(payload, mock_context)
    assert "reddit_client_id" in str(excinfo.value)
    mock_logger.error.assert_any_call(
        "Missing a required top-level key in payload (e.g., reddit_client_id, reddit_client_secret, or tasks): 'reddit_client_id'"
    )


def test_lambda_handler_invalid_task_missing_keys(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    mock_praw_reddit_class = mocker.patch("ingest_reddit.ingest_reddit.praw.Reddit")
    mocker.patch("ingest_reddit.ingest_reddit.fetch_reddit")
    mocker.patch("ingest_reddit.ingest_reddit.put_to_s3")
    mocker.patch("ingest_reddit.ingest_reddit.boto3.client")

    payload = {
        "reddit_client_id": "id",
        "reddit_client_secret": "secret",
        "tasks": [{"subreddit": "incomplete_task"}],
    }
    mock_context = MagicMock(
        function_name="f",
        function_version="v",
        aws_request_id="r",
        memory_limit_in_mb="m",
    )

    lambda_handler(payload, mock_context)

    mock_logger.error.assert_any_call(
        "Task 1 is missing 'subreddit' or 's3_bucket'. Skipping: {'subreddit': 'incomplete_task'}"
    )
    mock_praw_reddit_class.assert_called_once()
    mock_logger.info.assert_any_call(
        f"Lambda function {mock_context.function_name} version {mock_context.function_version} completed successfully."
    )


def test_lambda_handler_empty_tasks(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    mock_praw_reddit_class = mocker.patch("ingest_reddit.ingest_reddit.praw.Reddit")
    payload = {"reddit_client_id": "id", "reddit_client_secret": "secret", "tasks": []}
    mock_context = MagicMock(
        function_name="f",
        function_version="v",
        aws_request_id="r",
        memory_limit_in_mb="m",
    )
    lambda_handler(payload, mock_context)
    mock_logger.warning.assert_any_call("No tasks found in payload. Exiting.")
    mock_praw_reddit_class.assert_not_called()


def test_lambda_handler_tasks_not_a_list(mocker, mock_logger):
    mock_get_logger = mocker.patch("ingest_reddit.ingest_reddit.logging.getLogger")
    mock_get_logger.return_value = mock_logger
    mocker.patch("ingest_reddit.ingest_reddit.praw.Reddit")
    payload = {
        "reddit_client_id": "id",
        "reddit_client_secret": "secret",
        "tasks": "not-a-list",
    }
    mock_context = MagicMock(
        function_name="f",
        function_version="v",
        aws_request_id="r",
        memory_limit_in_mb="m",
    )
    with pytest.raises(TypeError) as excinfo:
        lambda_handler(payload, mock_context)
    assert str(excinfo.value) == "Payload 'tasks' must be a list."
    mock_logger.error.assert_any_call("Payload 'tasks' must be a list.")
