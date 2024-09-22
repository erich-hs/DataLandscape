import os
import logging
import boto3
from io import StringIO
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, UTC
from typing import Optional

class S3LogHandler(logging.Handler):
    def __init__(self, logger_name, s3_bucket, s3_log_dir, s3_client, log_file_name):
        super().__init__()
        self.logger_name = logger_name
        self.buffer = StringIO()
        self.s3_bucket = s3_bucket
        self.s3_log_dir = s3_log_dir
        self.s3_client = s3_client
        self.log_file_name = log_file_name

    def emit(self, record):
        log_entry = self.format(record)
        self.buffer.write(log_entry + '\n')
    
    def flush(self):
        if self.buffer.tell() > 0:
            log_key = f'{self.s3_log_dir}/{self.logger_name}/{self.log_file_name}'
            try:
                # Upload the buffer content to S3
                self.s3_client.put_object(Bucket=self.s3_bucket, Key=log_key, Body=self.buffer.getvalue(), ContentType='text/plain')
                self.buffer = StringIO()  # Clear the buffer after flushing
            except Exception as e:
                print(f'Failed to write log to {self.s3_bucket}/{log_key}: {e}')
    
    def close(self):
        self.flush()
        super().close()

def setup_s3_logging(
        logger_name: str,
        s3_bucket: str,
        s3_log_dir: str,
        s3_client: boto3.client,
        local_logs_dir: Optional[str] = None,
):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    log_file_name = f'{logger_name}_{datetime.now(UTC).strftime("%Y-%m-%d_%H-%M-%S")}.log'

    # Create console handler and set level to info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create custom S3 handler
    s3_handler = S3LogHandler(logger_name, s3_bucket, s3_log_dir, s3_client, log_file_name)
    s3_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    s3_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(s3_handler)

    # Local log file handler
    if local_logs_dir:
        if not os.path.exists(f"{local_logs_dir}/{logger_name}/"):
            Path(f"{local_logs_dir}/{logger_name}/").mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(f"{local_logs_dir}/{logger_name}/{log_file_name}", maxBytes=5*1024*1024, backupCount=5)
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger