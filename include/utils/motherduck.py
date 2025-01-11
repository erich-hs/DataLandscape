import duckdb
from aws_athena import athena_query_to_pyarrow


def load_iceberg_table_to_motherduck(
    table_name: str,
    database: str,
    database_schema: str | None,
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
    conn.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    if database_schema:
        conn.sql(f"CREATE SCHEMA IF NOT EXISTS {database}.{database_schema}")

    # Serialize PyArrow table in memory from Athena
    pa_table = athena_query_to_pyarrow(
        query=athena_dql_query,
        database=database,
        pa_schema=pa_schema
    )

    # Create MotherDuck table if it doesn't exist
    conn.sql(motherduck_ddl_query)

    # Load PyArrow table into MotherDuck Data Warehouse
    if motherduck_preload_query:
        conn.sql(motherduck_preload_query)
    conn.sql(f"INSERT INTO {database}{f'.{database_schema}.' if database_schema else '.'}{table_name} (SELECT * FROM pa_table)")
    if motherduck_postload_query:
        conn.sql(motherduck_postload_query)

    conn.close()