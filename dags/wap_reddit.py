from pendulum import duration
from airflow.decorators import dag
from airflow.sdk import chain

from include.utils.factories import glue_job_task_factory

# reddit_comments_raw = Asset("raw.reddit.comments", group="reddit")
# reddit_submissions_raw = Asset("raw.reddit.submissions", group="reddit")


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=duration(minutes=30),
    default_args={"retries": 2},
    description="Test WAP dag.",
    tags=["reddit", "wap", "raw"],
)
def wap_reddit_dag():
    TABLE_NAME = "first_model"
    DATABASE_NAME = "raw"
    AUDIT_BRANCH_NAME = "audit_branch"
    CREATE_BRANCH_SCRIPT_PATH = "include/utils/iceberg/spark_job_create_table_branch.py"
    PROCESS_REDDIT_THREADS_SCRIPT_PATH = (
        "include/spark/spark_job_process_reddit_threads.py"
    )
    AUDIT_REDDIT_THREADS_SCRIPT_PATH = "include/spark/spark_job_audit_reddit_threads.py"
    SPARK_WAP_JOB_CONFIGURATIONS = {
        "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-west-2:533267070818:bucket/dl-dev-s3tablebucket-raw01",
        "spark.wap.branch": AUDIT_BRANCH_NAME,
        "spark.jars.packages": "io.openlineage:openlineage-spark_2.12:1.26.0",
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": "https://oleander.dev",
        "spark.openlineage.transport.auth.type": "api_key",
        "spark.openlineage.transport.auth.apiKey": "***",
        "spark.openlineage.namespace": "datalandscape",
    }

    create_audit_branch = glue_job_task_factory(
        task_id="create_audit_branch",
        job_name="create_audit_branch",
        job_description="Create audit branch on first_model table.",
        script_path=CREATE_BRANCH_SCRIPT_PATH,
        run_arguments={
            "--table_name": TABLE_NAME,
            "--database_name": DATABASE_NAME,
            "--branch_name": AUDIT_BRANCH_NAME,
        },
    )

    process_reddit_threads = glue_job_task_factory(
        task_id="process_reddit_threads",
        job_name="process_reddit_threads",
        job_description="Process Reddit threads.",
        script_path=PROCESS_REDDIT_THREADS_SCRIPT_PATH,
        spark_configurations=SPARK_WAP_JOB_CONFIGURATIONS,
    )

    audit_reddit_threads = glue_job_task_factory(
        task_id="audit_reddit_threads",
        job_name="audit_reddit_threads",
        job_description="Audit Reddit threads.",
        script_path=AUDIT_REDDIT_THREADS_SCRIPT_PATH,
        spark_configurations=SPARK_WAP_JOB_CONFIGURATIONS,
        additional_python_modules=["pydeequ"],
    )

    chain(
        create_audit_branch(),
        process_reddit_threads(),
        audit_reddit_threads(),
    )


wap_reddit_dag()
