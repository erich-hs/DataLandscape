FROM public.ecr.aws/lambda/python:3.10

# Copy function code and requirements
COPY scrapping_job_reddit.py ${LAMBDA_TASK_ROOT}/
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt -t ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD [ "scrapping_job_reddit.lambda_handler" ] 