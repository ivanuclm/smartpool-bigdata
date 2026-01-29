
import os
from pyspark.sql import SparkSession

# =========
# MINIO / S3A
# =========
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "spark")

BASE   = f"s3a://{MINIO_BUCKET}/medallion"
BRONZE = f"{BASE}/bronze"
SILVER = f"{BASE}/silver"
GOLD   = f"{BASE}/gold"
STATE  = f"{BASE}/_state"

# =========
# SQL SERVER (JDBC)
# =========
JDBC_URL = os.getenv(
    "SMARTPOOL_JDBC_URL",
    "jdbc:sqlserver://sqlserver:1433;databaseName=smartpool;encrypt=true;trustServerCertificate=true;",
)
JDBC_USER = os.getenv("SMARTPOOL_JDBC_USER", "sa")
JDBC_PASS = os.getenv("SMARTPOOL_JDBC_PASS", "Password1234%")
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# =========
# SPARK
# =========
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# Prefer local jars (avoid Ivy downloads during execution)
EXTRA_JARS_LIST = [
    "/opt/spark/jars/hadoop-aws-3.4.0.jar",
    "/opt/spark/jars/hadoop-common-3.4.0.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar",
    "/opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar",
    "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
    "/opt/spark/jars/delta-storage-4.0.0.jar",
    "/opt/spark/jars/antlr4-runtime-4.13.1.jar",
    # Kafka (only needed by the streaming job; harmless if present)
    "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
    "/opt/spark/jars/kafka-clients-3.9.1.jar",
]

# Optional resource caps (set via env to keep defaults unchanged)
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES")
SPARK_EXECUTOR_INSTANCES = os.getenv("SPARK_EXECUTOR_INSTANCES")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY")
SPARK_CORES_MAX = os.getenv("SPARK_CORES_MAX")

def create_spark(app_name: str, extra_confs: dict | None = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)

        .config("spark.sql.session.timeZone", os.getenv("SPARK_TZ", "Europe/Madrid"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.adaptive.enabled", "false")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    )

    extra_jars = [p for p in EXTRA_JARS_LIST if os.path.exists(p)]
    if extra_jars:
        builder = builder.config("spark.jars", ",".join(extra_jars))

    if SPARK_EXECUTOR_MEMORY:
        builder = builder.config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    if SPARK_EXECUTOR_CORES:
        builder = builder.config("spark.executor.cores", SPARK_EXECUTOR_CORES)
    if SPARK_EXECUTOR_INSTANCES:
        builder = builder.config("spark.executor.instances", SPARK_EXECUTOR_INSTANCES)
    if SPARK_DRIVER_MEMORY:
        builder = builder.config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    if SPARK_CORES_MAX:
        builder = builder.config("spark.cores.max", SPARK_CORES_MAX)

    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
