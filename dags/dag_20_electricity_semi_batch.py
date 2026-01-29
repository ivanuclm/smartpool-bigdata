
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"

COMMON_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
}

COMMON_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.4.0.jar",
    "/opt/spark/jars/hadoop-common-3.4.0.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar",
    "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
    "/opt/spark/jars/delta-storage-4.0.0.jar",
    "/opt/spark/jars/antlr4-runtime-4.13.1.jar",
])

def _branch_if_has_run_date(**context):
    # Simple branching: always run (keeps it deterministic for delivery),
    # but we still demonstrate a conditional node as required.
    return "pick_ingest_date"

@task(task_id="pick_ingest_date")
def pick_ingest_date():
    ctx = get_current_context()
    return ctx["ds"]

with DAG(
    dag_id="smartpool_electricity_semi_batch",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Triggered by dag_10 (no fixed schedule)
    catchup=False,
    tags=["smartpool", "batch", "semi", "electricity"],
) as dag:

    choose_path = BranchPythonOperator(
        task_id="branch_run_or_skip",
        python_callable=_branch_if_has_run_date,
        # provide_context=True, # Error en airflow 3
    )

    skip = EmptyOperator(task_id="skip_electricity")

    ingest = SparkSubmitOperator(
        task_id="ingest_electricity_csv",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/05_ingest_electricity_csv.py",
        name="05_ingest_electricity_csv",
        jars=COMMON_JARS,
        conf=COMMON_CONF,
        application_args=["--ingest-date", "{{ ti.xcom_pull(task_ids='pick_ingest_date') }}"],
        verbose=False,
    )

    enrich = SparkSubmitOperator(
        task_id="electricity_gold_stats",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/06_gold_electricity_enrichment.py",
        name="06_gold_electricity_enrichment",
        jars=COMMON_JARS,
        conf=COMMON_CONF,
        application_args=["--target-date", "{{ ti.xcom_pull(task_ids='pick_ingest_date') }}"],
        verbose=False,
    )

    # end = EmptyOperator(task_id="end")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")


    choose_path >> pick_ingest_date() >> ingest >> enrich >> end
    choose_path >> skip >> end
