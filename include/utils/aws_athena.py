import time
import boto3
import pyarrow as pa
import pyarrow.compute as pc
from botocore.client import BaseClient

def submit_athena_query(
    athena_client: BaseClient,
    query: str,
    database: str
) -> dict:
    print(f"Submitting Athena query...:\n{query}")
    query_submit_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://mad-dashboard-s3-001/athena_results/'
        }
    )

    print(f"Polling query execution status for query {query_submit_response['QueryExecutionId']}...")
    while True:
        query_execution_response = athena_client.get_query_execution(
            QueryExecutionId=query_submit_response['QueryExecutionId']
        )

        if query_execution_response['QueryExecution']['Status']['State'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            print(f"Query status: {query_execution_response['QueryExecution']['Status']['State']}")
            break

        time.sleep(1)
    
    return query_submit_response

def get_athena_query_results(
    athena_client: BaseClient,
    query_submit_response: dict,
    next_token: str | None = None
) -> dict:
    if next_token:
        query_results = athena_client.get_query_results(
            QueryExecutionId=query_submit_response['QueryExecutionId'],
            MaxResults=1000,
            NextToken=next_token
        )
    else:
        query_results = athena_client.get_query_results(
            QueryExecutionId=query_submit_response['QueryExecutionId'],
            MaxResults=1000
        )

    return query_results

def query_rows_to_pyarrow(
    rows: list[dict],
    pa_schema: pa.Schema
) -> pa.Table:
    table_arrays = {}

    for i, column in enumerate(pa_schema):
        column_name = column.name
        column_type = column.type

        if column_type.equals(pa.date32()):
            # Use None for missing data
            table_arrays[column_name] = pc.strptime(
                [
                    row['Data'][i].get('VarCharValue', None) 
                    for row in rows
                ], 
                format="%Y-%m-%d", unit="s"
            )
        elif column_type.equals(pa.int32()):
            # Convert values to int, handle missing data as None
            table_arrays[column_name] = pa.array(
                [
                    int(row['Data'][i]['VarCharValue']) 
                    if 'VarCharValue' in row['Data'][i] else None 
                    for row in rows
                ], 
                type=column_type
            )
        elif column_type.equals(pa.string()):
            # Use None for missing data
            table_arrays[column_name] = pa.array(
                [
                    row['Data'][i].get('VarCharValue', None) 
                    for row in rows
                ], 
                type=column_type
            )
        elif column_type.equals(pa.float16()) or column_type.equals(pa.float32()) or column_type.equals(pa.float64()):
            # Convert values to float, handle missing data as None
            table_arrays[column_name] = pa.array(
                [
                    float(row['Data'][i]['VarCharValue']) 
                    if 'VarCharValue' in row['Data'][i] else None 
                    for row in rows
                ], 
                type=column_type
            )
        elif column_type.equals(pa.list_(pa.string())):
            # Convert values to list of strings, handle missing data as None
            table_arrays[column_name] = pa.array(
                [
                    row['Data'][i]['VarCharValue'][1:-1].split(', ') # Remove brackets and split by spaced comma
                    if 'VarCharValue' in row['Data'][i] else None
                    for row in rows
                ], 
                type=column_type
            )
        else:
            raise ValueError(f"Unsupported type: {column_type}")
    
    return pa.Table.from_arrays(list(table_arrays.values()), schema=pa_schema)


def athena_query_to_pyarrow(
    query: str,
    database: str,
    pa_schema: pa.Schema
) -> pa.Table:
    result_tables = []
    athena_client = boto3.client('athena')

    query_submit_response = submit_athena_query(athena_client=athena_client, query=query, database=database)
    query_results = get_athena_query_results(athena_client=athena_client, query_submit_response=query_submit_response)

    rows = query_results['ResultSet']['Rows'][1:]
    result_tables.append(query_rows_to_pyarrow(rows=rows, pa_schema=pa_schema))
    
    row_count = len(rows)
    print(f"Appended {row_count} rows to result table")

    while 'NextToken' in query_results:
        time.sleep(0.1)
        print("Paginating query results...")
        query_results = get_athena_query_results(athena_client=athena_client, query_submit_response=query_submit_response, next_token=query_results['NextToken'])
        rows = query_results['ResultSet']['Rows']
        result_tables.append(query_rows_to_pyarrow(rows=rows, pa_schema=pa_schema))
        
        row_count += len(rows)
        print(f"Appended {len(rows)} rows to result table. Total rows: {row_count}")
    
    return pa.concat_tables(result_tables)