
import argparse
import logging
from pyspark.sql import functions as F, Window

from smartpool_config import create_spark, BRONZE, SILVER

logger = logging.getLogger("03_silver_smartpool")

BRONZE_JDBC = f"{BRONZE}/smartpool_jdbc"
SILVER_POOLS  = f"{SILVER}/pools_dim"
SILVER_EVENTS = f"{SILVER}/maintenance_events"

def build_latest_by_modified(df, pk="id", modified="modified"):
    w = Window.partitionBy(pk).orderBy(F.col(modified).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

def run(spark):
    pools = spark.read.format("delta").load(f"{BRONZE_JDBC}/pools_dim")
    events = spark.read.format("delta").load(f"{BRONZE_JDBC}/maintenance_events")

    pools_latest = build_latest_by_modified(pools, pk="id", modified="modified")
    events_latest = build_latest_by_modified(events, pk="id", modified="modified")

    (
        pools_latest
        .withColumn("silver_ingest_ts", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_POOLS)
    )

    (
        events_latest
        .withColumn("silver_ingest_ts", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_EVENTS)
    )

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--noop", action="store_true", help="No-op (for testing wiring)")
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    if args.noop:
        logger.info("noop")
        return
    spark = create_spark("03_silver_smartpool")
    run(spark)
    spark.stop()

if __name__ == "__main__":
    main()
