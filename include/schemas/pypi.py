from typing import Union, List

def pypi_file_downloads_query(
    project: Union[str, List[str]],
    start_date: str,
    end_date: str
) -> str:
    if isinstance(project, str):
        project_where_clause = f"project = '{project}'"
    elif isinstance(project, list):
        project_where_clause = f"""project IN ({', '.join([f'"{p}"' for p in project])})"""
    return f"""SELECT
    CAST(date_trunc(timestamp, DAY) AS DATE) AS download_date,
    project,
    file.version AS project_version,
    details.python AS python,
    details.system.name AS system_name,
    country_code,
    COUNT(project) AS download_count,
FROM `bigquery-public-data.pypi.file_downloads`
WHERE
    {project_where_clause}
    AND TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP("{start_date}")
    AND TIMESTAMP_TRUNC(timestamp, DAY) < TIMESTAMP("{end_date}")
GROUP BY
    CAST(date_trunc(timestamp, DAY) AS DATE),
    project,
    file.version,
    country_code,
    details.python,
    details.system.name
"""

def pypi_file_downloads_create_table_query(
        target_table: str,
        location: str
) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
    download_date DATE,
    project STRING,
    project_version STRING,
    python STRING,
    system_name STRING,
    country_code STRING,
    download_count INT
)
PARTITIONED BY (download_date)
LOCATION 's3://{location}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
)
"""

def agg_pypi_cumulative_file_downloads_create_table_query(
        target_table: str,
        location: str
) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
    reference_date DATE,
    project STRING,
    project_version STRING,
    country_code STRING,
    download_count INT,
    download_count_last_7_days INT,
    download_count_last_30_days INT,
    download_count_last_90_days INT,
    download_count_last_365_days INT,
    download_count_since_first_record INT
)
COMMENT 'Aggregate table for PyPI file downloads with total download count per project, per project version, per country for today and for the last seven, last thirty and last ninety days.'
PARTITIONED BY (reference_date)
LOCATION 's3://{location}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
)
"""