import os
import boto3
from dotenv import load_dotenv

from ..utils.log_config import setup_s3_logging
from ..utils.aws_s3 import fetch_and_decompress_zst, put_to_s3
from .scraping_job_reddit import SUBREDDITS

def fetch_reddit_archive() -> None:
    load_dotenv()
    s3_bucket = os.getenv("AWS_S3_BUCKET")
    s3_client = boto3.client('s3')

    logger = setup_s3_logging(
        logger_name=f'reddit_archive',
        s3_bucket=s3_bucket,
        s3_log_dir='logs',
        s3_client=s3_client,
        local_logs_dir=f'.dev/logs'
    )

    for sub in SUBREDDITS:
        submissions_url = f"https://the-eye.eu/redarcs/files/{sub}_submissions.zst"
        comments_url = f"https://the-eye.eu/redarcs/files/{sub}_comments.zst"

        # Fetch and decompress the submissions and comments data
        submissions_data = fetch_and_decompress_zst(submissions_url, logger)
        comments_data = fetch_and_decompress_zst(comments_url, logger)

        # Put the data to S3
        if submissions_data:
            put_to_s3(
                data=submissions_data.getvalue(),
                s3_bucket=s3_bucket,
                s3_file=f"data/reddit/archive/{sub.lower()}_submissions.json",
                s3_client=s3_client,
                logger=logger
            )

        if comments_data:
            put_to_s3(
                data=comments_data.getvalue(),
                s3_bucket=s3_bucket,
                s3_file=f"data/reddit/archive/{sub.lower()}_comments.json",
                s3_client=s3_client,
                logger=logger
            )

if __name__ == "__main__":
    fetch_reddit_archive()