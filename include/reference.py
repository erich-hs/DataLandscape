POLARITY_CATEGORIES = {
    'Very Negative': (-1.0, -0.6),
    'Negative': (-0.599, -0.2),
    'Neutral': (-0.199, 0.199),
    'Positive': (0.2, 0.599),
    'Very Positive': (0.6, 1.0)
}

SUBREDDITS = [
    "dataengineering",
    "MachineLearning",
    "datascience",
    "analytics",
    "LocalLLaMA",
    "learnprogramming",
]

TRACKED_PROJECTS_JSON = [{'project_name': 'Delta Lake',
  'project_search_terms': ['Delta Lake', 'Delta Table', 'Delta Format'],
  'pypi': 'deltalake',
  'project_categories': ['Open Table Format']},
 {'project_name': 'Apache Iceberg',
  'project_search_terms': ['Iceberg'],
  'pypi': 'pyiceberg',
  'project_categories': ['Open Table Format']},
 {'project_name': 'Apache Hudi',
  'project_search_terms': ['Hudi'],
  'pypi': 'hudi',
  'project_categories': ['Open Table Format']},
 {'project_name': 'Apache Avro',
  'project_search_terms': ['Avro'],
  'pypi': 'avro',
  'project_categories': ['Serialization Format', 'File Format']},
 {'project_name': 'Apache Arrow',
  'project_search_terms': ['Arrow', 'PyArrow'],
  'pypi': 'pyarrow',
  'project_categories': ['Serialization Format']},
 {'project_name': 'Apache Parquet',
  'project_search_terms': ['Parquet'],
  'pypi': None,
  'project_categories': ['File Format']},
 {'project_name': 'Apache ORC',
  'project_search_terms': ['ORC'],
  'pypi': 'pyorc',
  'project_categories': ['File Format']},
 {'project_name': 'Apache Thrift',
  'project_search_terms': ['Thrift'],
  'pypi': 'thrift',
  'project_categories': ['Data Protocol']},
 {'project_name': 'Protobuf',
  'project_search_terms': ['Protobuf', 'Protocol Buffers'],
  'pypi': 'protobuf',
  'project_categories': ['Serialization Format', 'Data Protocol']},
 {'project_name': 'Apache Kafka',
  'project_search_terms': ['Kafka'],
  'pypi': 'kafka',
  'project_categories': ['Streaming Framework', 'Data Storage']},
 {'project_name': 'Apache Flink',
  'project_search_terms': ['Flink'],
  'pypi': 'apache-flink',
  'project_categories': ['Streaming Framework', 'Data Processing Engine']},
 {'project_name': 'Confluent Kafka',
  'project_search_terms': ['Confluent'],
  'pypi': 'confluent-kafka',
  'project_categories': ['Streaming Platform', 'Data Processing Platform']},
 {'project_name': 'Redpanda',
  'project_search_terms': ['Redpanda'],
  'pypi': None,
  'project_categories': ['Streaming Platform', 'Data Processing Platform']},
 {'project_name': 'Striim',
  'project_search_terms': ['Striim'],
  'pypi': None,
  'project_categories': ['Streaming Platform', 'Data Processing Platform']},
 {'project_name': 'PostgreSQL',
  'project_search_terms': ['Postgres'],
  'pypi': 'psycopg2',
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'MySQL',
  'project_search_terms': ['MySQL'],
  'pypi': 'mysql-connector-python',
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'SQLServer',
  'project_search_terms': ['SQL Server'],
  'pypi': None,
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'SQLite',
  'project_search_terms': ['SQLite'],
  'pypi': None,
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'DynamoDB',
  'project_search_terms': ['Dynamo DB'],
  'pypi': None,
  'project_categories': ['NoSQL Database', 'Data Storage']},
 {'project_name': 'MongoDB',
  'project_search_terms': ['Mongo DB'],
  'pypi': 'pymongo',
  'project_categories': ['NoSQL Database', 'Data Storage']},
 {'project_name': 'CosmosDB',
  'project_search_terms': ['Cosmos DB'],
  'pypi': None,
  'project_categories': ['NoSQL Database', 'Graph Database', 'Data Storage']},
 {'project_name': 'ScyllaDB',
  'project_search_terms': ['Scylla DB'],
  'pypi': 'scylla-driver',
  'project_categories': ['NoSQL Database', 'Data Storage']},
 {'project_name': 'Apache Druid',
  'project_search_terms': ['Druid'],
  'pypi': 'pydruid',
  'project_categories': ['OLAP Database', 'Data Storage']},
 {'project_name': 'ClickHouse',
  'project_search_terms': ['ClickHouse'],
  'pypi': 'clickhouse-connect',
  'project_categories': ['OLAP Database', 'Data Storage']},
 {'project_name': 'Apache Pinot',
  'project_search_terms': ['Pinot'],
  'pypi': 'pinotdb',
  'project_categories': ['OLAP Database', 'Data Storage']},
 {'project_name': 'DuckDB',
  'project_search_terms': ['Duck DB'],
  'pypi': 'duckdb',
  'project_categories': ['OLAP Database', 'Data Processing Engine']},
 {'project_name': 'Amazon Redshift',
  'project_search_terms': ['Redshift'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Data Storage']},
 {'project_name': 'Google BigQuery',
  'project_search_terms': ['BigQuery'],
  'pypi': 'google-cloud-bigquery',
  'project_categories': ['Data Warehouse', 'Data Storage']},
 {'project_name': 'Azure Synapse Analytics',
  'project_search_terms': ['Synapse'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Databricks',
  'project_search_terms': ['Databricks'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Snowflake',
  'project_search_terms': ['Snowflake'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Dremio',
  'project_search_terms': ['Dremio'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Starburst',
  'project_search_terms': ['Starburst'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Microsoft Fabric',
  'project_search_terms': ['Microsoft Fabric', 'MS Fabric'],
  'pypi': None,
  'project_categories': ['Data Warehouse', 'Analytics Platform']},
 {'project_name': 'Couchbase',
  'project_search_terms': ['Couchbase'],
  'pypi': 'couchbase',
  'project_categories': ['NoSQL Database', 'Data Storage']},
 {'project_name': 'CockroachDB',
  'project_search_terms': ['Cockroach DB'],
  'pypi': None,
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'MariaDB',
  'project_search_terms': ['Maria DB'],
  'pypi': 'mariadb',
  'project_categories': ['Relational Database', ' Data Storage']},
 {'project_name': 'Apache Cassandra',
  'project_search_terms': ['Cassandra'],
  'pypi': None,
  'project_categories': ['NoSQL Database', 'Data Storage']},
 {'project_name': 'Redis',
  'project_search_terms': ['Redis'],
  'pypi': 'redis',
  'project_categories': ['In-Memory Database']},
 {'project_name': 'Neptune',
  'project_search_terms': ['Neptune'],
  'pypi': 'neptune',
  'project_categories': ['Graph Database', 'Data Storage']},
 {'project_name': 'Neo4j',
  'project_search_terms': ['Neo 4j'],
  'pypi': 'neo4j',
  'project_categories': ['Graph Database', 'Data Storage']},
 {'project_name': 'JanusGraph',
  'project_search_terms': ['Janus Graph'],
  'pypi': None,
  'project_categories': ['Graph Database', 'Data Storage']},
 {'project_name': 'ArangoDB',
  'project_search_terms': ['Arango DB'],
  'pypi': 'pyarango',
  'project_categories': ['Graph Database', 'Data Storage']},
 {'project_name': 'Pinecone',
  'project_search_terms': ['Pinecone'],
  'pypi': 'pinecone',
  'project_categories': ['Vector Database', 'Data Storage']},
 {'project_name': 'Weaviate',
  'project_search_terms': ['Weaviate'],
  'pypi': 'weaviate-client',
  'project_categories': ['Vector Database', 'Data Storage']},
 {'project_name': 'Qdrant',
  'project_search_terms': ['Qdrant'],
  'pypi': 'qdrant-client',
  'project_categories': ['Vector Database', 'Data Storage']},
 {'project_name': 'Chroma',
  'project_search_terms': ['Chroma'],
  'pypi': 'chromadb',
  'project_categories': ['Vector Database', 'Data Storage']},
 {'project_name': 'TimescaleDB',
  'project_search_terms': ['Timescale DB'],
  'pypi': None,
  'project_categories': ['Time Series Database', 'Data Storage']},
 {'project_name': 'InfluxDB',
  'project_search_terms': ['Influx DB'],
  'pypi': 'influxdb-client',
  'project_categories': ['Time Series Database', 'Data Storage']},
 {'project_name': 'Apache Spark',
  'project_search_terms': ['Spark'],
  'pypi': 'pyspark',
  'project_categories': ['Data Processing Engine']},
 {'project_name': 'Trino',
  'project_search_terms': ['Trino'],
  'pypi': 'trino',
  'project_categories': ['Data Processing Engine']},
 {'project_name': 'Presto',
  'project_search_terms': ['Presto'],
  'pypi': 'presto-python-client',
  'project_categories': ['Data Processing Engine']},
 {'project_name': 'dbt Labs',
  'project_search_terms': ['dbt'],
  'pypi': 'dbt-core',
  'project_categories': ['Data Transformation Framework']},
 {'project_name': 'SDF Labs',
  'project_search_terms': ['SDF'],
  'pypi': 'sdf-cli',
  'project_categories': ['Data Transformation Framework',
   'Data Processing Engine']},
 {'project_name': 'Fivetran',
  'project_search_terms': ['Fivetran'],
  'pypi': None,
  'project_categories': ['Data Integration Engine',
   'Data Integration Platform']},
 {'project_name': 'Airbyte',
  'project_search_terms': ['Airbyte'],
  'pypi': 'airbyte',
  'project_categories': ['Data Integration Engine',
   'Data Integration Platform']},
 {'project_name': 'Meltano',
  'project_search_terms': ['Meltano'],
  'pypi': 'meltano',
  'project_categories': ['Data Integration Engine']},
 {'project_name': 'AWS Glue',
  'project_search_terms': ['Glue'],
  'pypi': None,
  'project_categories': ['Data Integration Platform',
   'Data Processing Platform']},
 {'project_name': 'Azure Data Factory',
  'project_search_terms': ['Data Factory', 'ADF'],
  'pypi': None,
  'project_categories': ['Data Integration Platform',
   'Data Processing Platform']},
 {'project_name': 'Google Cloud Dataflow',
  'project_search_terms': ['Dataflow'],
  'pypi': None,
  'project_categories': ['Data Integration Platform',
   'Data Processing Platform']},
 {'project_name': 'Apache Airflow',
  'project_search_terms': ['Airflow'],
  'pypi': 'apache-airflow',
  'project_categories': ['Orchestration Engine']},
 {'project_name': 'Dagster',
  'project_search_terms': ['Dagster'],
  'pypi': 'dagster',
  'project_categories': ['Orchestration Engine']},
 {'project_name': 'Prefect',
  'project_search_terms': ['Prefect'],
  'pypi': 'prefect',
  'project_categories': ['Orchestration Engine']},
 {'project_name': 'Mage',
  'project_search_terms': ['Mage'],
  'pypi': 'mage-ai',
  'project_categories': ['Data Integration Engine', 'Orchestration Engine']},
 {'project_name': 'Astronomer',
  'project_search_terms': ['Astronomer'],
  'pypi': None,
  'project_categories': ['Orchestration Platform']},
 {'project_name': 'Collibra',
  'project_search_terms': ['Collibra'],
  'pypi': 'collibra-core',
  'project_categories': ['Data Catalog', 'Data Governance']},
 {'project_name': 'Monte Carlo',
  'project_search_terms': ['Monte Carlo Data', 'MonteCarlo'],
  'pypi': None,
  'project_categories': ['Data Governance']},
 {'project_name': 'Soda',
  'project_search_terms': ['Soda'],
  'pypi': 'soda-core',
  'project_categories': ['Data Governance']},
 {'project_name': 'Microsoft Purview',
  'project_search_terms': ['Purview'],
  'pypi': None,
  'project_categories': ['Data Governance']},
 {'project_name': 'Pandas',
  'project_search_terms': ['Pandas'],
  'pypi': 'pandas',
  'project_categories': ['Data Processing Engine', 'Data Frame API']},
 {'project_name': 'Polars',
  'project_search_terms': ['Polars'],
  'pypi': 'polars',
  'project_categories': ['Data Processing Engine', 'Data Frame API']},
 {'project_name': 'Ibis',
  'project_search_terms': ['Ibis'],
  'pypi': 'ibis-framework',
  'project_categories': ['Data Processing Engine', 'Data Frame API']},
 {'project_name': 'PyTorch',
  'project_search_terms': ['PyTorch'],
  'pypi': 'torch',
  'project_categories': ['Machine Learning Framework']},
 {'project_name': 'TensorFlow',
  'project_search_terms': ['TensorFlow'],
  'pypi': 'tensorflow',
  'project_categories': ['Machine Learning Framework']},
 {'project_name': 'Keras',
  'project_search_terms': ['Keras'],
  'pypi': 'keras',
  'project_categories': ['Machine Learning Framework']},
 {'project_name': 'Scikit-Learn',
  'project_search_terms': ['Scikit Learn', 'SKLearn'],
  'pypi': 'scikit-learn',
  'project_categories': ['Machine Learning Framework']},
 {'project_name': 'XGBoost',
  'project_search_terms': ['XGBoost'],
  'pypi': 'xgboost',
  'project_categories': ['Gradient Boosting Algorithm']},
 {'project_name': 'LightGBM',
  'project_search_terms': ['LightGBM'],
  'pypi': 'lightgbm',
  'project_categories': ['Gradient Boosting Algorithm']},
 {'project_name': 'Google Vertex AI',
  'project_search_terms': ['Vertex'],
  'pypi': None,
  'project_categories': ['Machine Learning Platform']},
 {'project_name': 'Amazon Sagemaker',
  'project_search_terms': ['Sagemaker'],
  'pypi': None,
  'project_categories': ['Machine Learning Platform']},
 {'project_name': 'Azure Machine Learning',
  'project_search_terms': ['Azure Machine Learning', 'Azure ML'],
  'pypi': None,
  'project_categories': ['Machine Learning Platform']},
 {'project_name': 'MLFlow',
  'project_search_terms': ['ML Flow'],
  'pypi': 'mlflow',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'Optuna',
  'project_search_terms': ['Optuna'],
  'pypi': 'optuna',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'Weights & Biases',
  'project_search_terms': ['Weights & Biases', 'Weights and Biases', 'wandb'],
  'pypi': 'wandb',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'dvc',
  'project_search_terms': ['dvc'],
  'pypi': 'dvc',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'Kubeflow',
  'project_search_terms': ['Kubeflow'],
  'pypi': 'kfp',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'ZenML',
  'project_search_terms': ['Zen ML'],
  'pypi': 'zenml',
  'project_categories': ['MLOps Framework']},
 {'project_name': 'LangChain',
  'project_search_terms': ['LangChain'],
  'pypi': 'langchain',
  'project_categories': ['LLM Integration Framework']},
 {'project_name': 'LlamaIndex',
  'project_search_terms': ['Llama Index'],
  'pypi': 'llama-index',
  'project_categories': ['LLM Integration Framework']},
 {'project_name': 'DSPy',
  'project_search_terms': ['DSPy'],
  'pypi': 'dspy-ai',
  'project_categories': ['LLM Prompt Optimization Framework']},
 {'project_name': 'LiteLLM',
  'project_search_terms': ['LiteLLM'],
  'pypi': 'litellm',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'LMQL',
  'project_search_terms': ['LMQL'],
  'pypi': 'lmql',
  'project_categories': ['LLM Prompt Optimization Framework']},
 {'project_name': 'Haystack',
  'project_search_terms': ['Haystack'],
  'pypi': 'haystack-ai',
  'project_categories': ['LLM Integration Framework']},
 {'project_name': 'Ollama',
  'project_search_terms': ['Ollama'],
  'pypi': 'ollama',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'LlamaCpp',
  'project_search_terms': ['Llama Cpp'],
  'pypi': 'llama-cpp-python',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'OpenLLM',
  'project_search_terms': ['OpenLLM'],
  'pypi': 'openllm',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'AdalFlow',
  'project_search_terms': ['AdalFlow'],
  'pypi': 'adalflow',
  'project_categories': ['LLM Prompt Optimization Framework']},
 {'project_name': 'vLLM',
  'project_search_terms': ['vLLM'],
  'pypi': 'vllm',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'LitServe',
  'project_search_terms': ['LitServe'],
  'pypi': 'litserve',
  'project_categories': ['LLM Serving Engine']},
 {'project_name': 'Tableau',
  'project_search_terms': ['Tableau'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Microsoft PowerBI',
  'project_search_terms': ['PowerBI'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Looker',
  'project_search_terms': ['Looker'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Amazon QuickSight',
  'project_search_terms': ['QuickSight'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Apache Superset',
  'project_search_terms': ['Superset'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Evidence.dev',
  'project_search_terms': ['Evidence.dev'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Airtable',
  'project_search_terms': ['Airtable'],
  'pypi': None,
  'project_categories': ['Dashboard and BI Platform']},
 {'project_name': 'Microsoft Excel',
  'project_search_terms': ['Excel'],
  'pypi': None,
  'project_categories': ['Spreadsheets Application']},
 {'project_name': 'Google Sheets',
  'project_search_terms': ['Google Sheets'],
  'pypi': None,
  'project_categories': ['Spreadsheets Application']},
 {'project_name': 'Unity Catalog',
  'project_search_terms': ['Unity Catalog'],
  'pypi': None,
  'project_categories': ['Data Catalog']},
 {'project_name': 'Polaris Catalog',
  'project_search_terms': ['Polaris'],
  'pypi': None,
  'project_categories': ['Data Catalog']},
 {'project_name': 'Nessie Catalog',
  'project_search_terms': ['Nessie'],
  'pypi': None,
  'project_categories': ['Data Catalog']},
 {'project_name': 'Hive Metastore',
  'project_search_terms': ['Hive Metastore'],
  'pypi': None,
  'project_categories': ['Data Catalog']},
 {'project_name': 'AWS Glue Catalog',
  'project_search_terms': ['Glue Catalog'],
  'pypi': None,
  'project_categories': ['Data Catalog']}]