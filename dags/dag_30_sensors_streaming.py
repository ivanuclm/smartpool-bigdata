
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_CONN_ID = "spark_default"

def _map_to_daily(execution_date, **_):
    # Align hourly runs to the daily structured batch logical date (00:00)
    return execution_date.start_of("day")

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
    "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
    "/opt/spark/jars/kafka-clients-3.9.1.jar",
])

with DAG(
    dag_id="smartpool_sensors_streaming",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly", # macro temporal
    catchup=False,
    tags=["smartpool", "streaming", "kafka"],
) as dag:

    wait_structured = ExternalTaskSensor(
        task_id="wait_structured_batch",
        external_dag_id="smartpool_structured_batch",
        external_task_id="build_silver",
        execution_date_fn=_map_to_daily,
        timeout=60 * 60,
        mode="reschedule",
    )

    run_stream = SparkSubmitOperator(
        task_id="run_kafka_stream_job",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/07_kafka_smartpool_sensors.py",
        name="07_kafka_smartpool_sensors",
        jars=COMMON_JARS,
        conf=COMMON_CONF,
        application_args=[
            "--bootstrap", "kafka:9092",
            "--topic", "smartpool-sensors",
            "--run-seconds", "120",
            "--trigger-seconds", "10",
            "--watermark", "2 minutes",
        ],
        verbose=False,
    )

    done = EmptyOperator(task_id="done")

    wait_structured >> run_stream >> done
