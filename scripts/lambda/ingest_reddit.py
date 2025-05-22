import time

def lambda_handler(payload, context):
    print(f"Hello from Lambda function {context.function_name} version {context.function_version}!")
    print("Lambda function ARN:", context.invoked_function_arn)
    print("CloudWatch log stream name:", context.log_stream_name)
    print("CloudWatch log group name:",  context.log_group_name)
    print("Lambda Request ID:", context.aws_request_id)
    print("Lambda function memory limits in MB:", context.memory_limit_in_mb)
    # We have added a 1 second delay so you can see the time remaining in get_remaining_time_in_millis.
    print(payload)
    time.sleep(1) 
    print("Lambda time remaining in MS:", context.get_remaining_time_in_millis())
