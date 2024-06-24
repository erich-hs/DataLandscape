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
    timestamp,
    country_code,
    project,
    file.version AS file_version,
    file.type AS file_type,
    details.installer.name AS installer_name,
    details.installer.version AS installer_version,
    details.python AS python,
    details.implementation.name AS implementation_name,
    details.implementation.version AS implementation_version,
    details.distro.name AS distro_name,
    details.distro.version AS distro_version,
    details.system.name AS system_name,
    details.system.release AS system_release,
    details.cpu AS cpu,
    details.openssl_version AS openssl_version,
    details.setuptools_version AS setuptools_version
FROM `bigquery-public-data.pypi.file_downloads`
WHERE
    {project_where_clause}
    AND TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP("{start_date}")
    AND TIMESTAMP_TRUNC(timestamp, DAY) < TIMESTAMP("{end_date}")
"""

pypi_file_downloads_schema = """
    timestamp TIMESTAMP,
    country_code STRING,
    project STRING,
    file_version STRING,
    file_type STRING,
    installer_name STRING,
    installer_version STRING,
    python STRING,
    implementation_name STRING,
    implementation_version STRING,
    distro_name STRING,
    distro_version STRING,
    system_name STRING,
    system_release STRING,
    cpu STRING,
    openssl_version STRING,
    setuptools_version STRING
"""