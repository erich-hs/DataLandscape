import os
from dotenv import load_dotenv
from include.utils.aws_glue import submit_glue_job

load_dotenv()

# Job parameters
job_name = 'backfill_pypi_file_downloads'
script_path = 'include/scripts/spark/spark_job_ingest_pypi.py'
job_description = 'Ingest PyPI file downloads from Google BigQuery public dataset.'
run_arguments = {
    "--ds": "2024-06-20",
    "--date_end": "2024-06-21",
    "--pypi_project": "duckdb",
    "--target_table": "pypi_file_downloads_backfill"
}

# AWS parameters
s3_bucket = os.getenv('AWS_S3_BUCKET')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Google BigQuery dependencies
additional_python_modules=[
    'google-api-core==2.19.0',
    'google-auth==2.29.0',
    'google-cloud-bigquery==3.24.0',
    'google-cloud-bigquery-storage==2.25.0',
    'google-cloud-core==2.4.1',
    'google-crc32c==1.5.0',
    'google-resumable-media==2.7.0',
    'googleapis-common-protos==1.63.1'
]

local_log_dir = '.dev/logs'

if __name__ == "__main__":
    submit_glue_job(
        job_name=job_name,
        job_description=job_description,
        run_arguments=run_arguments,
        script_path=script_path,
        s3_bucket=s3_bucket,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        additional_python_modules=additional_python_modules,
        local_log_dir=local_log_dir
   )