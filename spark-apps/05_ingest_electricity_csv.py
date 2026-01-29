
import argparse
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, StringType

from smartpool_config import create_spark, BRONZE, SILVER, MINIO_BUCKET

logger = logging.getLogger("05_ingest_electricity_csv")

LANDING_ELEC = f"s3a://{MINIO_BUCKET}/landing/electricity_prices"
BRONZE_ELEC  = f"{BRONZE}/electricity_prices_raw"
SILVER_ELEC  = f"{SILVER}/electricity_prices"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ingest-date", default=None, help="Optional ingest date (YYYY-MM-DD). If not set, uses current_date().")
    return p.parse_args()

def run(spark, ingest_date: str | None):
    read_path = LANDING_ELEC
    if ingest_date:
        read_path = f"{LANDING_ELEC}/date={ingest_date}"

    df = spark.read.option("header", "true").csv(read_path)
    cols = set(df.columns)

    if "price_eur_mwh" in cols:
        df = df.withColumn("price_eur_mwh", F.col("price_eur_mwh").cast(DoubleType()))
    if "price_eur_kwh" in cols:
        df = df.withColumn("price_eur_kwh", F.col("price_eur_kwh").cast(DoubleType()))

    if "ts" in cols:
        df = df.withColumn("ts", F.to_timestamp("ts").cast(TimestampType()))
    elif "ts_utc" in cols:
        df = df.withColumn("ts", F.to_timestamp("ts_utc").cast(TimestampType()))
    elif "date" in cols and "hour" in cols:
        df = df.withColumn(
            "ts",
            F.to_timestamp(
                F.concat_ws(" ", F.col("date"), F.lpad(F.col("hour").cast(StringType()), 2, "0")),
                "yyyy-MM-dd HH",
            ).cast(TimestampType()),
        )
    else:
        raise ValueError("No timestamp column found (expected ts, ts_utc, or date+hour).")

    df = (
        df.withColumn("source_file", F.input_file_name().cast(StringType()))
          .withColumn("_ingest_ts", F.current_timestamp())
    )

    if ingest_date:
        df = df.withColumn("ingest_date", F.to_date(F.lit(ingest_date)))
    else:
        df = df.withColumn("ingest_date", F.current_date())

    if "date" in cols:
        df = df.withColumn("date", F.col("date").cast(StringType()))
    else:
        df = df.withColumn("date", F.date_format("ts", "yyyy-MM-dd"))

    bronze_df = df
    if "ingest_date" in bronze_df.columns:
        bronze_df = bronze_df.withColumn("ingest_date", F.col("ingest_date").cast(StringType()))
    if "date" in bronze_df.columns:
        bronze_df = bronze_df.withColumn("date", F.col("date").cast(StringType()))

    (
        bronze_df.write.format("delta").mode("append")
        .save(BRONZE_ELEC)
    )

    # Silver: basic cleanup + partition by date
    silver = (
        bronze_df.select("ts", "price_eur_mwh", "source_file", "ingest_date", "date", "_ingest_ts")
          .withColumn("ingest_date", F.to_date("ingest_date"))
          .withColumn("date", F.to_date("date"))
          .filter(F.col("ts").isNotNull() & F.col("price_eur_mwh").isNotNull())
    )

    (
        silver.write.format("delta")
        .mode("append")
        .partitionBy("date")
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
