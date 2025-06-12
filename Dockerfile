FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Set up isolated dbt environment
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-trino dbt-core && \
    deactivate