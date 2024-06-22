import logging
from typing import List, Union
import duckdb
import pandas as pd
import pyarrow as pa
import polars as pl

class DuckDBManager:
    def __init__(
            self,
            database: str,
            logger: logging.Logger,
    ):
        self.database = database
        self.connection = duckdb.connect(database)
        self.logger = logger
    
    def attach_motherduck(self, motherduck_token):
        self.logger.info(f"Attaching MotherDuck to {self.database}")
        try:
            self.connection.sql("INSTALL md;")
            self.connection.sql("LOAD md;")
            self.connection.sql(f"SET motherduck_token='{motherduck_token}';")
            self.connection.sql(f"ATTACH 'md:'")
        except Exception as e:
            self.logger.error(f"Failed to attach MotherDuck: {e}")
            raise e

    def set_aws_credentials(self, aws_key_id, aws_secret, aws_region):
        self.logger.info(f"Setting AWS credentials for {self.database}")
        try:
            self.connection.sql(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{aws_key_id}',
                    SECRET '{aws_secret}',
                    REGION '{aws_region}'
                );
            """)
        except Exception as e:
            self.logger.error(f"Failed to set AWS credentials: {e}")
            raise e

    def sql(self, query: str):
        self.logger.info(f"Executing query at {self.database}:\n{query}")
        return self.connection.sql(query)

    def close(self):
        self.connection.close()

def write_table_to_s3(
    table: pa.Table,
    table_name: str,
    duckdb_schema: str,
    duckdb_manager: DuckDBManager,
    s3_bucket: str,
    format: str = 'parquet',
    partition_cols: List[str] = []
):
    pa_table = table
    # Create DuckDB table in memory
    duckdb_manager.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({duckdb_schema})")
    duckdb_manager.sql(f"INSERT INTO {table_name} SELECT * FROM pa_table")