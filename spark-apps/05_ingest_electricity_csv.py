
import argparse
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, StringType

from smartpool_config import create_spark, BRONZE, SILVER, MINIO_BUCKET

logger = logging.getLogger("05_ingest_electricity_csv")

LANDING_ELEC = f"s3a://{MINIO_BUCKET}/landing/electricity"
BRONZE_ELEC  = f"{BRONZE}/electricity_prices_raw"
SILVER_ELEC  = f"{SILVER}/electricity_prices"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ingest-date", default=None, help="Optional ingest date (YYYY-MM-DD). If not set, uses current_date().")
    return p.parse_args()

def run(spark, ingest_date: str | None):
    df = (
        spark.read.option("header", "true").csv(LANDING_ELEC)
        .withColumn("price_eur_mwh", F.col("price_eur_mwh").cast(DoubleType()))
        .withColumn("ts", F.to_timestamp("ts").cast(TimestampType()))
        .withColumn("source_file", F.input_file_name().cast(StringType()))
        .withColumn("_ingest_ts", F.current_timestamp())
    )

    if ingest_date:
        df = df.withColumn("ingest_date", F.lit(ingest_date))
    else:
        df = df.withColumn("ingest_date", F.current_date().cast("string"))

    (
        df.write.format("delta").mode("append")
        .save(BRONZE_ELEC)
    )

    # Silver: basic cleanup + partition by ingest_date
    silver = (
        df.select("ts", "price_eur_mwh", "source_file", "ingest_date", "_ingest_ts")
          .filter(F.col("ts").isNotNull() & F.col("price_eur_mwh").isNotNull())
    )

    (
        silver.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(SILVER_ELEC)
    )

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    spark = create_spark("05_ingest_electricity_csv")
    run(spark, args.ingest_date)
    spark.stop()

if __name__ == "__main__":
    main()
