import pytest # type: ignore
import json
import os
from unittest import mock
from moto import mock_aws # type: ignore
import boto3 # type: ignore
from freezegun import freeze_time # type: ignore

# Import the Lambda handler and other functions we want to test
# We need to add the parent directory (lambda root) to sys.path to import the module
import sys
lambda_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if lambda_root_path not in sys.path:
    sys.path.insert(0, lambda_root_path)

from scrapping_job_reddit import lambda_handler, fetch_reddit, RedditSubmission, RedditComment # type: ignore

# Mock PRAW client and its methods
@pytest.fixture
def mock_praw_reddit():
    with mock.patch('praw.Reddit') as mock_reddit_class:
        mock_reddit_instance = mock_reddit_class.return_value
        
        # Mock subreddit method
        mock_subreddit = mock.Mock()
        mock_reddit_instance.subreddit.return_value = mock_subreddit
        
        # Mock submission object and its comments
        mock_submission = mock.Mock()
        mock_submission.id = "test_submission_id"
        mock_submission.title = "Test Submission"
        mock_submission.author_fullname = "t2_testuser"
        mock_submission.created_utc = 1678886400.0 # Example timestamp
        mock_submission.archived = False
        mock_submission.author_flair_css_class = None
        mock_submission.author_flair_text = None
        mock_submission.category = None
        mock_submission.clicked = False
        mock_submission.created = 1678886400.0
        mock_submission.distinguished = None
        mock_submission.domain = "self.python"
        mock_submission.downs = 0
        mock_submission.edited = False
        mock_submission.gilded = 0
        mock_submission.gildings = {}
        mock_submission.hidden = False
        mock_submission.hide_score = False
        mock_submission.is_created_from_ads_ui = False
        mock_submission.is_meta = False
        mock_submission.is_original_content = False
        mock_submission.is_reddit_media_domain = False
        mock_submission.is_self = True
        mock_submission.is_video = False
        mock_submission.link_flair_css_class = None
        mock_submission.link_flair_text = None
        mock_submission.locked = False
        mock_submission.media = None
        mock_submission.media_embed = {}
        mock_submission.media_only = False
        mock_submission.name = "t3_test_submission_id"
        mock_submission.no_follow = True
        mock_submission.num_comments = 1
        mock_submission.num_crossposts = 0
        mock_submission.over_18 = False
        mock_submission.permalink = "/r/python/comments/test_submission_id/test_submission/"
        mock_submission.pinned = False
        mock_submission.pwls = 6
        mock_submission.quarantine = False
        mock_submission.removed_by_category = None
        mock_submission.saved = False
        mock_submission.score = 100
        mock_submission.secure_media = None
        mock_submission.selftext = "This is a test submission."
        mock_submission.selftext_html = "<p>This is a test submission.</p>"
        mock_submission.send_replies = True
        mock_submission.spoiler = False
        mock_submission.stickied = False
        mock_submission.subreddit_id = "t5_2qh0u"
        mock_submission.subreddit_name_prefixed = "r/python"
        mock_submission.subreddit_subscribers = 1000000
        mock_submission.subreddit_type = "public"
        mock_submission.thumbnail = "self"
        mock_submission.title = "Test Submission Title"
        mock_submission.total_awards_received = 0
        mock_submission.treatment_tags = []
        mock_submission.ups = 100
        mock_submission.upvote_ratio = 0.99
        mock_submission.url = "https://www.reddit.com/r/python/comments/test_submission_id/test_submission/"
        mock_submission.user_reports = []
        mock_submission.wls = 6
        mock_submission.__dict__.update(mock_submission.__dict__) # Ensure __dict__ is populated for Pydantic
        mock_subreddit.new.return_value = [mock_submission] # .new() returns a list of submissions

        # Mock comment object
        mock_comment = mock.Mock()
        mock_comment.id = "test_comment_id"
        mock_comment.author_fullname = "t2_commenter"
        mock_comment.created_utc = 1678886500.0
        mock_comment.archived = False
        mock_comment.author_flair_css_class = None
        mock_comment.author_flair_text = None
        mock_comment.awarders = []
        mock_comment.body = "This is a test comment."
        mock_comment.body_html = "<p>This is a test comment.</p>"
        mock_comment.collapsed = False
        mock_comment.collapsed_reason = None
        mock_comment.collapsed_reason_code = None
        mock_comment.controversiality = 0
        mock_comment.distinguished = None
        mock_comment.downs = 0
        mock_comment.edited = False
        mock_comment.gilded = 0
        mock_comment.gildings = {}
        mock_comment.is_submitter = False
        mock_comment.link_id = "t3_test_submission_id"
        mock_comment.locked = False
        mock_comment.name = "t1_test_comment_id"
        mock_comment.no_follow = True
        mock_comment.parent_id = "t3_test_submission_id"
        mock_comment.permalink = "/r/python/comments/test_submission_id/test_submission/test_comment_id/"
        mock_comment.score = 10
        mock_comment.score_hidden = False
        mock_comment.send_replies = True
        mock_comment.stickied = False
        mock_comment.subreddit_id = "t5_2qh0u"
        mock_comment.subreddit_name_prefixed = "r/python"
        mock_comment.subreddit_type = "public"
        mock_comment.total_awards_received = 0
        mock_comment.treatment_tags = []
        mock_comment.ups = 10
        mock_comment.__dict__.update(mock_comment.__dict__)
        
        # Mock submission.comments.list() and replace_more()
        mock_submission.comments = mock.Mock()
        mock_submission.comments.list.return_value = [mock_comment]
        mock_submission.comments.replace_more.return_value = [] # Assuming no more comments

        # Mock reddit.submission() to return our mock_submission
        mock_reddit_instance.submission.return_value = mock_submission

        yield mock_reddit_instance

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        yield client

@pytest.fixture
def mock_lambda_context():
    """Creates a mock Lambda context object."""
    class MockContext:
        def __init__(self):
            self.function_name = "test_lambda_function"
            self.function_version = "$LATEST"
            self.invoked_function_arn = "arn:aws:lambda:us-west-2:123456789012:function:test_lambda_function"
            self.memory_limit_in_mb = 128
            self.aws_request_id = "test_aws_request_id"
            self.log_group_name = "/aws/lambda/test_lambda_function"
            self.log_stream_name = "2024/03/15/[$LATEST]abcdef1234567890"

        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes
    return MockContext()

@freeze_time("2024-03-15 12:00:00 UTC")
def test_lambda_handler_success(s3_client, mock_praw_reddit, mock_lambda_context):
    bucket_name = "test-s3-bucket-reddit"
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

    payload = {
        'subreddits': ['python'],
        's3_bucket': bucket_name,
        'reddit_client_id': 'test_client_id',
        'reddit_client_secret': 'test_client_secret'
    }

    response = lambda_handler(payload, mock_lambda_context)

    assert response['statusCode'] == 200
    assert response['body'] == 'Reddit data fetched successfully'

    # Verify S3 uploads
    expected_date_utc = "2024-03-15"
    # Submissions
    submissions_key = f"data/reddit/api/{expected_date_utc}/valid/python_submissions.json"
    response = s3_client.get_object(Bucket=bucket_name, Key=submissions_key)
    submissions_data = response['Body'].read().decode('utf-8')
    assert len(submissions_data.splitlines()) == 1 # One submission
    submission_record = json.loads(submissions_data.splitlines()[0])
    assert submission_record['id'] == "test_submission_id"
    assert submission_record['_fetched_date'] == expected_date_utc
    assert submission_record['edited'] is None # was False, converted to None

    # Comments
    comments_key = f"data/reddit/api/{expected_date_utc}/valid/python_comments.json"
    response = s3_client.get_object(Bucket=bucket_name, Key=comments_key)
    comments_data = response['Body'].read().decode('utf-8')
    assert len(comments_data.splitlines()) == 1 # One comment
    comment_record = json.loads(comments_data.splitlines()[0])
    assert comment_record['id'] == "test_comment_id"
    assert comment_record['_fetched_date'] == expected_date_utc
    assert comment_record['edited'] is None # was False, converted to None

    # Verify no invalid files were created (as we mock valid data)
    with pytest.raises(s3_client.exceptions.NoSuchKey):
        s3_client.get_object(Bucket=bucket_name, Key=f"data/reddit/api/{expected_date_utc}/invalid/python_submissions.json")
    with pytest.raises(s3_client.exceptions.NoSuchKey):
        s3_client.get_object(Bucket=bucket_name, Key=f"data/reddit/api/{expected_date_utc}/invalid/python_comments.json")

def test_lambda_handler_missing_payload_keys(mock_lambda_context):
    payload = {
        'subreddits': ['python']
        # Missing other required keys
    }
    with pytest.raises(KeyError) as excinfo:
        lambda_handler(payload, mock_lambda_context)
    assert "'s3_bucket'" in str(excinfo.value) # Example check

@freeze_time("2024-03-15 12:00:00 UTC")
@mock.patch('scrapping_job_reddit.fetch_subreddit_newest_submissions')
@mock.patch('scrapping_job_reddit.fetch_submission_comments')
def test_fetch_reddit_handles_praw_exceptions(
    mock_fetch_comments, mock_fetch_submissions, s3_client, mock_praw_reddit, caplog
):
    bucket_name = "test-s3-bucket-exceptions"
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    
    # Simulate PRAW API exception during submission fetching
    from praw.exceptions import RedditAPIException
    mock_fetch_submissions.side_effect = RedditAPIException([mock.Mock(type='SUBREDDIT_NOT_FOUND', message='Subreddit not found', field='subreddit')])

    logger_mock = mock.MagicMock()
    with pytest.raises(RedditAPIException):
        fetch_reddit(
            subreddits=['nonexistentsub'],
            s3_bucket=bucket_name,
            reddit_client_id='test_id',
            reddit_client_secret='test_secret',
            logger=logger_mock
        )
    
    # Check that an error was logged by the fetch_subreddit_newest_submissions (or the wrapper if it catches)
    # This depends on where the exception is caught and logged. 
    # For this example, assuming fetch_subreddit_newest_submissions itself logs and re-raises.
    # The current code re-raises, so lambda_handler would catch and log.

    # Reset mock and test comment fetching exception
    mock_fetch_submissions.reset_mock()
    mock_fetch_submissions.return_value = ([
        # Return a mock submission dictionary structure, similar to what Pydantic expects
        {
            'id': 'fake_id',
            'title': 'Fake Title',
            'author_fullname': 't2_fakeuser',
            'created_utc': 1678886400.0,
            'archived': False, 'author_flair_css_class': None, 'author_flair_text': None, 'category': None,
            'clicked': False, 'created': 1678886400.0, 'distinguished': None, 'domain': 'self.python',
            'downs': 0, 'edited': False, 'gilded': 0, 'gildings': {}, 'hidden': False, 'hide_score': False,
            'is_created_from_ads_ui': False, 'is_meta': False, 'is_original_content': False, 
            'is_reddit_media_domain': False, 'is_self': True, 'is_video': False, 'link_flair_css_class': None,
            'link_flair_text': None, 'locked': False, 'media': None, 'media_embed': {}, 'media_only': False,
            'name': 't3_fake_id', 'no_follow': True, 'num_comments': 0, 'num_crossposts': 0, 'over_18': False,
            'permalink': '/r/python/comments/fake_id/', 'pinned': False, 'pwls': 6, 'quarantine': False,
            'removed_by_category': None, 'saved': False, 'score': 10, 'secure_media': None, 
            'selftext': 'text', 'selftext_html': '<p>text</p>', 'send_replies': True, 'spoiler': False, 
            'stickied': False, 'subreddit_id': 't5_2qh0u', 'subreddit_name_prefixed': 'r/python',
            'subreddit_subscribers': 10000, 'subreddit_type': 'public', 'thumbnail': 'self',
            'total_awards_received': 0, 'treatment_tags': [], 'ups': 10, 'upvote_ratio': 1.0, 
            'url': 'http://fake.url', 'user_reports': [], 'wls': 6,
            '_fetched_date': '2024-03-15', '_fetched_iso_utc': '2024-03-15T12:00:00+00:00'
        }
    ], [])
    mock_fetch_comments.side_effect = RedditAPIException([mock.Mock(type='SOME_ERROR', message='Error fetching comments', field='comments')])

    with pytest.raises(RedditAPIException):
        fetch_reddit(
            subreddits=['python'],
            s3_bucket=bucket_name,
            reddit_client_id='test_id',
            reddit_client_secret='test_secret',
            logger=logger_mock
        )

# Add more tests for fetch_subreddit_newest_submissions and fetch_submission_comments directly if needed,
# especially for error handling and validation logic. 