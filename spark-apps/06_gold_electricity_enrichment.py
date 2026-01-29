
import argparse
import logging
from pyspark.sql import functions as F

from smartpool_config import create_spark, SILVER, GOLD

logger = logging.getLogger("06_gold_electricity_enrichment")

SILVER_ELEC = f"{SILVER}/electricity_prices"

GOLD_ELEC_DAILY = f"{GOLD}/electricity_daily_stats"
GOLD_ELEC_PEAK  = f"{GOLD}/electricity_peak_hours"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--target-date", default=None, help="Optional ingest_date partition to process (YYYY-MM-DD).")
    return p.parse_args()

def run(spark, target_date: str | None):
    df = spark.read.format("delta").load(SILVER_ELEC)
    if target_date:
        df = df.filter(F.col("ingest_date") == F.to_date(F.lit(target_date)))

    if df.rdd.isEmpty():
        logger.info("No electricity data to process for target_date=%s", target_date)
        return

    df = df.withColumn("date", F.to_date("ts")).withColumn("hour", F.hour("ts"))

    daily = (
        df.groupBy("date")
          .agg(
              F.avg("price_eur_mwh").alias("avg_price"),
              F.max("price_eur_mwh").alias("max_price"),
              F.min("price_eur_mwh").alias("min_price"),
              F.count("*").alias("num_rows"),
          )
          .withColumn("gold_calc_ts", F.current_timestamp())
    )

    (
        daily.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_ELEC_DAILY)
    )

    peak = (
        df.groupBy("date", "hour")
          .agg(F.avg("price_eur_mwh").alias("avg_price"))
          .withColumn("rank", F.dense_rank().over(
              __import__("pyspark").sql.window.Window.partitionBy("date").orderBy(F.col("avg_price").desc())
          ))
          .filter(F.col("rank") <= 3)
          .drop("rank")
          .withColumn("gold_calc_ts", F.current_timestamp())
    )

    (
        peak.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_ELEC_PEAK)
    )

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    spark = create_spark("06_gold_electricity_enrichment")
    run(spark, args.target_date)
    spark.stop()

if __name__ == "__main__":
    main()
