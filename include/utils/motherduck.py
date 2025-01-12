import duckdb
from .aws_athena import athena_query_to_pyarrow


def load_iceberg_table_to_motherduck(
    table_name: str,
    athena_database: str,
    motherduck_database: str,
    motherduck_database_schema: str | None,
    athena_dql_query: str,
    motherduck_ddl_query: str,
    motherduck_preload_query: str | None,
    motherduck_postload_query: str | None,
    pa_schema: str,
    motherduck_token: str
):
    # MotherDuck connection
    conn = duckdb.connect()
    conn.sql("INSTALL md;")
    conn.sql("LOAD md;")
    conn.sql(f"SET motherduck_token='{motherduck_token}';")
    conn.sql("ATTACH 'md:'")

    # Initialize MotherDuck Data Warehouse if it doesn't exist
    conn.sql(f"CREATE DATABASE IF NOT EXISTS {motherduck_database}")
    if motherduck_database_schema:
        conn.sql(f"CREATE SCHEMA IF NOT EXISTS {motherduck_database}.{motherduck_database_schema}")

    # Serialize PyArrow table in memory from Athena
    print(f"Submitting Athena query...\n{athena_dql_query}")
    pa_table = athena_query_to_pyarrow(
        query=athena_dql_query,
        database=athena_database,
        pa_schema=pa_schema
    )

    # Create MotherDuck table if it doesn't exist
    conn.sql(motherduck_ddl_query)

    # Load PyArrow table into MotherDuck Data Warehouse
    if motherduck_preload_query:
        conn.sql(motherduck_preload_query)
    conn.sql(f"INSERT INTO {motherduck_database}{f'.{motherduck_database_schema}.' if motherduck_database_schema else '.'}{table_name} (SELECT * FROM pa_table)")
    if motherduck_postload_query:
        conn.sql(motherduck_postload_query)

    conn.close()