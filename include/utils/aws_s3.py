import os
import logging
import requests
import zstandard as zstd
from io import BytesIO
from botocore.exceptions import NoCredentialsError, ClientError

def upload_to_s3(local_file, s3_bucket, s3_file, s3_client, logger):
    try:
        # An upload_file will overwrite the file if it already exists
        s3_client.upload_file(local_file, s3_bucket, s3_file)
        logger.info(f"Upload to S3 successful: {local_file} to {s3_bucket}/{s3_file}")
        return f's3://{s3_bucket}/{s3_file}'
    except FileNotFoundError as e:
        logger.error("The file was not found")
        raise e
    except NoCredentialsError as e:
        logger.error("Credentials not available")
        raise e
    except ClientError as e:
        logger.error(f"Client error: {e}")
        raise e

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

def fetch_and_decompress_zst(
    url: str,
    logger: logging.Logger
) -> BytesIO:
    # Instantiate a Zstandard decompressor
    dctx = zstd.ZstdDecompressor()

    # Log the beginning of the download
    logger.info(f"Starting download from {url}")

    # Fetch the data from the URL
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if response.status_code == 404:
            logger.warn(f"Failed to download from {url}: 404 Not Found")
        else:
            logger.error(f"Failed to download from {url}: {e}")
            raise e

    # Initialize bytes read counter
    bytes_read = 0

    # Initialize a buffer to store the decompressed data
    decompressed_data = BytesIO()

    # Decompress the data and write it to the buffer
    if response.status_code == 200:
        with dctx.stream_reader(response.raw) as reader:
            while True:
                chunk = reader.read(4096)
                if not chunk:
                    break
                decompressed_data.write(chunk)
                bytes_read += len(chunk)

    logger.info(f"Download from {url} completed. Total bytes read: {bytes_read}")
    
    # Reset the buffer to the beginning
    decompressed_data.seek(0)
    
    return decompressed_data

def write_to_file(
    data: BytesIO,
    target_dir: str,
    target_file: str,
    logger: logging.Logger,
) -> None:
    logger.info(f"Starting write to {os.path.join(target_dir, target_file)}")
    
    # Initialize record counter
    record_count = 0

    # Write the decompressed data to the target file
    try:
        with open(os.path.join(target_dir, target_file), 'wb') as f:
            for line in data:
                f.write(line)
                record_count += 1
    except Exception as e:
        logger.error(f"Failed to write to {os.path.join(target_dir, target_file)}: {e}")
        raise e
  
    # Log successful write with record count
    logger.info(f"Write to {os.path.join(target_dir, target_file)} completed. Record Count: {record_count}")