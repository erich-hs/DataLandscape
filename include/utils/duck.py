import os
import logging
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
    
    def _attach_motherduck(self, motherduck_token):
        self.logger.info(f"Attaching MotherDuck to {self.database}")
        try:
            self.connection.sql("INSTALL md;")
            self.connection.sql("LOAD md;")
            self.connection.sql(f"SET motherduck_token='{motherduck_token}';")
            self.connection.sql(f"ATTACH 'md:'")
        except Exception as e:
            self.logger.error(f"Failed to attach MotherDuck: {e}")
            raise e

    def _set_aws_credentials(self, aws_key_id, aws_secret, aws_region):
        self.logger.info(f"Setting AWS credentials to {self.database}")
        try:
            self.connection.sql(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{aws_key_id}',
                    SECRET '{aws_secret}',
                    REGION '{aws_region}'
                )
            """)
        except Exception as e:
            self.logger.error(f"Failed to set AWS credentials: {e}")
            raise e

    def sql(self, query: str):
        self.logger.info(f"Executing query at {self.database}:\n{query}")
        return self.connection.sql(query)

    def load_pyarrow_table(
        self,
        table: pa.Table,
        table_name: str,
        table_schema: str,
    ):
        pa_table = table
        self.logger.info(f"Loading PyArrow table {table_name} ({_bytes_to_human_readable(pa_table.nbytes)}) to {self.database}")

        # Create DuckDB table from PyArrow table
        try:
            self.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})")
            self.sql(f"INSERT INTO {table_name} SELECT * FROM pa_table")
        except Exception as e:
            self.logger.error(f"Failed to load PyArrow table to {self.database}.{table_name}: {e}")
            raise e
        self.logger.info(f"Table {table_name} created at {self.database}")

    def write_table_to_s3(
            self,
            table_name: str,
            s3_bucket: str,
            s3_dir: str = None,
            date_partition_col: str = None,
            partition_by: str = None,
        ):
        aws_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION")

        self._set_aws_credentials(aws_key_id, aws_secret, aws_region)

        partition_sql = ""
        partition_clause = ""
        if date_partition_col:
            if partition_by == 'year':
                partition_sql = f", YEAR({date_partition_col}) AS year"
                partition_clause = " PARTITION_BY (year),"
            elif partition_by == 'month':
                partition_sql = f", YEAR({date_partition_col}) AS year, MONTH({date_partition_col}) AS month"
                partition_clause = " PARTITION_BY (year, month),"
            elif partition_by == 'date':
                partition_sql = f", YEAR({date_partition_col}) AS year, MONTH({date_partition_col}) AS month, DAY({date_partition_col}) AS day"
                partition_clause = " PARTITION_BY (year, month, day),"


        self.logger.info(f"Writing table {table_name} from {self.database} to s3://{s3_bucket}/{s3_dir}")
        if date_partition_col:
            self.connection.sql(f"""
                COPY (
                    SELECT
                        *
                        {partition_sql}
                    FROM {table_name}
                )
                TO 's3://{s3_bucket}/{s3_dir}/{table_name}'
                (FORMAT 'parquet',{partition_clause if partition_clause else ''} OVERWRITE_OR_IGNORE 1)
            """)
        self.logger.info(f"Table written at s3://{s3_bucket}/{s3_dir}/{table_name}")

    def flush(self):
        #TODO: Implement a method to flush the database
        self.connection.sql("")

    def close(self):
        self.connection.close()

def _bytes_to_human_readable(num_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024