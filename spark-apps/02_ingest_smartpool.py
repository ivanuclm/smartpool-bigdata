
import argparse
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from smartpool_config import (
    create_spark,
    BRONZE,
    STATE,
    JDBC_URL, JDBC_USER, JDBC_PASS, JDBC_DRIVER,
)

logger = logging.getLogger("02_ingest_smartpool")

BRONZE_JDBC = f"{BRONZE}/smartpool_jdbc"
STATE_JDBC  = f"{STATE}/smartpool_jdbc_last_execution"

DEFAULT_TABLES = ["pools_dim", "maintenance_events"]

def jdbc_read(spark, table, predicates=None):
    reader = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", JDBC_DRIVER)
    )
    if predicates:
        reader = reader.option("predicates", predicates)
    return reader.load()

def read_last_execution(spark, path):
    try:
        return DeltaTable.forPath(spark, path).toDF().select("last_execution").first()["last_execution"]
    except Exception:
        return None

def write_last_execution(spark, path, ts):
    df = spark.createDataFrame([(ts,)], ["last_execution"])
    df.write.format("delta").mode("overwrite").save(path)

def ingest_table_incremental(
    spark,
    table: str,
    modified_col: str = "updated_at",
    pk_col: str = "pool_id",
):
    last_exec = read_last_execution(spark, f"{STATE_JDBC}/{table}")
    base_query = f"(SELECT * FROM {table}) AS t"

    if last_exec:
        query = f"(SELECT * FROM {table} WHERE {modified_col} > '{last_exec}') AS t"
        logger.info("Incremental read %s since %s", table, last_exec)
    else:
        query = base_query
        logger.info("Full load %s (no checkpoint yet)", table)

    df = jdbc_read(spark, query)

    if df.rdd.isEmpty():
        logger.info("No new rows for %s", table)
        return 0

    out_path = f"{BRONZE_JDBC}/{table}"
    (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .write.format("delta")
          .mode("append")
          .save(out_path)
    )

    max_mod = df.agg(F.max(F.col(modified_col)).alias("mx")).first()["mx"]
    if max_mod:
        write_last_execution(spark, f"{STATE_JDBC}/{table}", str(max_mod))
    return df.count()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--tables", default=",".join(DEFAULT_TABLES),
                  help="Comma-separated table list (default: pools_dim,maintenance_events)")
    p.add_argument("--modified-col", default="updated_at")
    p.add_argument("--pk-col", default="pool_id")
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    spark = create_spark("02_ingest_smartpool")

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    total = 0
    for t in tables:
        try:
            total += ingest_table_incremental(spark, t, args.modified_col, args.pk_col)
        except Exception:
            logger.exception("Failed ingest for table %s", t)
            raise

    logger.info("Done. Total ingested rows (sum of per-table counts): %s", total)
    spark.stop()

if __name__ == "__main__":
    main()
