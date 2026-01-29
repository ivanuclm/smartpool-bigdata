
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    "/opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar",
    "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
    "/opt/spark/jars/delta-storage-4.0.0.jar",
    "/opt/spark/jars/antlr4-runtime-4.13.1.jar",
])

COMMON_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.4.0",
    "software.amazon.awssdk:bundle:2.23.19",
    "io.delta:delta-spark_2.13:4.0.0",
    "com.microsoft.sqlserver:mssql-jdbc:12.10.2.jre11",
])


with DAG(
    dag_id="smartpool_structured_batch",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",  # cron
    catchup=False,
    tags=["smartpool", "batch", "structured"],
) as dag:

    ingest_jdbc = SparkSubmitOperator(
        task_id="ingest_sqlserver_to_bronze",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/02_ingest_smartpool.py",
        name="02_ingest_smartpool",
        # jars=COMMON_JARS,
        packages=COMMON_PACKAGES,
        conf=COMMON_CONF,
        application_args=["--tables", "pools_dim,maintenance_events"],
        verbose=False,
    )

    build_silver = SparkSubmitOperator(
        task_id="build_silver",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/03_silver_smartpool.py",
        name="03_silver_smartpool",
        # jars=COMMON_JARS,
        packages=COMMON_PACKAGES,
        conf=COMMON_CONF,
        verbose=False,
    )

    build_gold = SparkSubmitOperator(
        task_id="build_gold",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/04_gold_smartpool.py",
        name="04_gold_smartpool",
        # jars=COMMON_JARS,
        packages=COMMON_PACKAGES,
        conf=COMMON_CONF,
        verbose=False,
    )

    trigger_electricity = TriggerDagRunOperator(
        task_id="trigger_electricity_dag",
        trigger_dag_id="smartpool_electricity_semi_batch",
        wait_for_completion=False,  # push to another DAG
    )

    ingest_jdbc >> build_silver >> build_gold >> trigger_electricity
