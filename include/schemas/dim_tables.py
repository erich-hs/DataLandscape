def dim_tracked_subreddits_create_table_query(
    target_table: str,
    location: str
) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
    subreddit_name STRING,
    subreddit_url STRING
)
LOCATION 's3://{location}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
)
"""

def dim_tracked_projects_create_table_query(
    target_table: str,
    location: str
) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
    project_name STRING,
    project_search_terms ARRAY<STRING>,
    pypi_project_name STRING,
    project_categories ARRAY<STRING>
)
LOCATION 's3://{location}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
)
"""

def dim_polarity_categories_create_table_query(
    target_table: str,
    location: str
) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {target_table} (
    polarity_category STRING,
    polarity_category_order INT,
    polarity_lower_bound DOUBLE,
    polarity_upper_bound DOUBLE
)
LOCATION 's3://{location}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
)
"""