from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType, LongType, BooleanType
)
from pyspark.sql.utils import AnalysisException

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

import os
import sys

# Usar SIEMPRE el mismo Python que está ejecutando el notebook / script
# PYTHON_BIN = sys.executable

# os.environ["PYSPARK_PYTHON"] = PYTHON_BIN
# os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_BIN

# =========
# MINIO / S3A
# =========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "spark"

BASE   = f"s3a://{MINIO_BUCKET}/medallion"
BRONZE = f"{BASE}/bronze"
SILVER = f"{BASE}/silver"
GOLD   = f"{BASE}/gold"
STATE  = f"{BASE}/_state"

JDBC_URL = (
    "jdbc:sqlserver://sqlserver:1433;"
    "databaseName=smartpool;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

JDBC_USER = "sa"
JDBC_PASS = "Password1234%"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


# =========
# JARS EXTRA
# =========
# EXTRA_JARS = ",".join([
#     "/opt/spark/jars/hadoop-aws-3.4.0.jar",
#     "/opt/spark/jars/hadoop-common-3.4.0.jar",
#     "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar",
#     "/opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar",
#     "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
#     "/opt/spark/jars/delta-storage-4.0.0.jar",
#     "/opt/spark/jars/antlr4-runtime-4.13.1.jar",
#     "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar",
#     # "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
#     "/opt/spark/jars/kafka-clients-3.9.1.jar",
# ])

# SPARK_PACKAGES = ",".join([
#     "org.apache.spark:spark-sql-kafka-0-10_2.13-4.0.1.jar",
#     "org.apache.spark:spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
    
# ])

# =========
# PACKAGES (Ivy/Maven)
# OJO: aquí van COORDENADAS, sin ".jar"
# =========
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1",
    "io.delta:delta-spark_2.13:4.0.0",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "software.amazon.awssdk:bundle:2.24.6",
])

# =========
# JARS LOCALES (solo lo que NO metas en packages)
# =========
EXTRA_JARS = ",".join([
    "/opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar",
])

    # "/opt/spark/jars/hadoop-aws-3.4.0.jar",
    # "/opt/spark/jars/hadoop-common-3.4.0.jar",
    # "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar",
    # "/opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar",
    # "/opt/spark/jars/delta-spark_2.13-4.0.0.jar",
    # "/opt/spark/jars/delta-storage-4.0.0.jar",
    # "/opt/spark/jars/antlr4-runtime-4.13.1.jar",
    # "org.apache.spark:spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
    # "org.apache.spark:kafka-clients-3.9.1.jar",
def create_spark(app_name: str = "smartpool"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        # .master("local[*]")
        .master("spark://spark-master:7077")
        
        # .config("spark.driver.memory", "1g")
        # .config("spark.driver.maxResultSize", "128m")

        # .config("spark.executor.instances", "2")
        # .config("spark.executor.cores", "1")
        # .config("spark.executor.memory", "1g")
        # .config("spark.executor.memoryOverhead", "512m")

        # .config("spark.sql.shuffle.partitions", "4")
        # .config("spark.sql.adaptive.enabled", "true")
        # .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
    
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")


        # Jars extra
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
         # "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6"
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.jars", EXTRA_JARS)
        
        #Forzamos el binario de Python para driver y workers
        # .config("spark.pyspark.python", PYTHON_BIN)
        # .config("spark.pyspark.driver.python", PYTHON_BIN)
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark