#!/bin/bash

# Script to build and invoke the Lambda function locally using Docker.

# Attempt to load .env file if it exists in the script's directory
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loading environment variables from $SCRIPT_DIR/.env"
  export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

# Configuration
IMAGE_NAME="ingest-reddit-lambda-local"
LAMBDA_DIR="./"
HANDLER_FUNCTION="scrapping_job_reddit.lambda_handler"

# Check for required environment variables
if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" || -z "$AWS_DEFAULT_REGION" || -z "$REDDIT_CLIENT_ID" || -z "$REDDIT_CLIENT_SECRET" || -z "S3_BUCKET_NAME" ]]; then
  echo "Error: Required environment variables are not set."
  echo "Please set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and S3_BUCKET_NAME."
  exit 1
fi

# Build the Docker image
echo "Building Docker image $IMAGE_NAME..."
cd "$LAMBDA_DIR"
docker build -t "$IMAGE_NAME" .
if [ $? -ne 0 ]; then
    echo "Docker build failed."
    exit 1
fi
cd -

# Payload for the Lambda function (modify as needed)
PAYLOAD='{ 
    "subreddits": ["python", "datascience"],
    "s3_bucket": "'"$S3_BUCKET_NAME"'",
    "reddit_client_id": "'"$REDDIT_CLIENT_ID"'",
    "reddit_client_secret": "'"$REDDIT_CLIENT_SECRET"'"
}'

echo "Invoking Lambda function $HANDLER_FUNCTION locally..."
echo "Payload: $PAYLOAD"

docker run \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
    -e REDDIT_CLIENT_ID="$REDDIT_CLIENT_ID" \
    -e REDDIT_CLIENT_SECRET="$REDDIT_CLIENT_SECRET" \
    --rm "$IMAGE_NAME" \
    "$HANDLER_FUNCTION" \
    "$PAYLOAD"

echo "Local invocation complete." 