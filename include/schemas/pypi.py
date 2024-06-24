from typing import Union, List

def pypi_file_downloads_query(
    project: Union[str, List[str]],
    start_date: str,
    end_date: str
) -> str:
    if isinstance(project, str):
        project_where_clause = f"project = '{project}'"
    elif isinstance(project, list):
        project_where_clause = f"project IN ({', '.join([f'"{p}"' for p in project])})"
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

pypi_file_downloads_schema = """
    download_date DATE,
    project STRING,
    project_version STRING,
    python STRING,
    system_name STRING,
    country_code STRING,
    download_count INT
"""