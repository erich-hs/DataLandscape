# Project Structure

This document outlines the structure of the DataLandscape project.

*   `.`: Root directory
    *   `dags/`: Contains Airflow DAGs.
        *   `ingest_reddit_to_s3_dag.py`: DAG to ingest Reddit data to S3.
        *   `load_reddit_to_raw_dag.py`: DAG to load Reddit data to raw layer.
    *   `include/`: Contains reusable components.
        *   `spark/`: Spark jobs.
            *   `spark_job_load_reddit_from_s3.py`: Spark job to load Reddit data from S3.
        *   `utils/`: Utility scripts.
            *   `aws_glue.py`: AWS Glue utility functions.
    *   `lambdas/`: Contains AWS Lambda functions.
        *   `ingest_reddit/`: Lambda function to ingest Reddit data.
            *   `ingest_reddit.py`: The main script for the lambda.
        *   `template.yaml`: SAM template for deploying Lambda functions.
    *   `tests/`: Contains tests for the project.
    *   `plugins/`: Contains Airflow plugins.
    *   `Dockerfile`: Dockerfile for the project.
    *   `Makefile`: Makefile for the project.
    *   `requirements.txt`: Python dependencies.
    *   `packages.txt`: System packages.
    *   `airflow_settings.yaml`: Airflow settings.
    *   `.pre-commit-config.yaml`: Pre-commit hooks configuration.
