
import argparse
import logging
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, TimestampType
)

from smartpool_config import create_spark, BRONZE, SILVER, GOLD

logger = logging.getLogger("07_kafka_smartpool_sensors")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default="smartpool-sensors")
    p.add_argument("--run-seconds", type=int, default=120, help="How long to keep streaming queries running.")
    p.add_argument("--trigger-seconds", type=int, default=10)
    p.add_argument("--watermark", default="2 minutes")
    return p.parse_args()

def build_schema():
    return StructType([
        StructField("pool_id", IntegerType(), False),
        StructField("sensor_ts", TimestampType(), False),
        StructField("ph", DoubleType(), True),
        StructField("chlorine_mg_l", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("turbidity_ntu", DoubleType(), True),
        StructField("water_level_pct", DoubleType(), True),
        StructField("pump_kwh_est", DoubleType(), True),
    ])

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    extra_confs = {
        "spark.streaming.stopGracefullyOnShutdown": "true",
    }
    spark = create_spark("07_kafka_smartpool_sensors", extra_confs=extra_confs)

    bronze_path = f"{BRONZE}/smartpool_sensors_kafka"
    silver_path = f"{SILVER}/smartpool_sensors"
    gold_1m_path = f"{GOLD}/smartpool_sensors_1m"
    gold_enr_path = f"{GOLD}/smartpool_sensors_enriched"

    chk_base = f"{BRONZE.split('/bronze')[0]}/_checkpoints"  # s3a://bucket/medallion/_checkpoints
    bronze_chk = f"{chk_base}/bronze_smartpool_sensors_kafka"
    silver_chk = f"{chk_base}/silver_smartpool_sensors"
    gold_agg_chk = f"{chk_base}/gold_smartpool_sensors_1m"
    gold_enr_chk = f"{chk_base}/gold_smartpool_sensors_enriched"

    schema = build_schema()

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    bronze_df = kafka_raw.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
    )

    q_bronze = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_chk)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("bronze_smartpool_sensors_kafka")
        .start(bronze_path)
    )

    parsed = (
        bronze_df
        .select(F.from_json(F.col("value"), schema).alias("j"))
        .select("j.*")
        .withWatermark("sensor_ts", args.watermark)
        .withColumn("ingest_date", F.to_date("sensor_ts"))
        .withColumn("silver_ingest_ts", F.current_timestamp())
    )

    q_silver = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", silver_chk)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("silver_smartpool_sensors")
        .start(silver_path)
    )

    # Aggregation (1-minute tumbling window)
    gold_agg = (
        parsed
        .groupBy(
            F.col("pool_id"),
            F.window("sensor_ts", "1 minute").alias("w"),
        )
        .agg(
            F.avg("ph").alias("ph_avg"),
            F.avg("chlorine_mg_l").alias("chlorine_avg"),
            F.avg("temp_c").alias("temp_avg"),
            F.max("turbidity_ntu").alias("turbidity_max"),
            F.avg("water_level_pct").alias("water_level_avg"),
            F.sum("pump_kwh_est").alias("pump_kwh_sum"),
            F.count("*").alias("num_readings"),
        )
        .select(
            "pool_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "ph_avg", "chlorine_avg", "temp_avg", "turbidity_max",
            "water_level_avg", "pump_kwh_sum", "num_readings",
            (F.col("ph_avg") < 7.1).cast("boolean").alias("ph_out_of_range_low"),
            (F.col("ph_avg") > 7.8).cast("boolean").alias("ph_out_of_range_high"),
            (F.col("chlorine_avg") < 0.4).cast("boolean").alias("chlorine_out_of_range_low"),
            (F.col("chlorine_avg") > 1.5).cast("boolean").alias("chlorine_out_of_range_high"),
            F.current_timestamp().alias("calc_ts"),
            F.current_date().alias("calc_date"),
        )
    )

    q_gold_agg = (
        gold_agg.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", gold_agg_chk)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("gold_smartpool_sensors_1m")
        .start(gold_1m_path)
    )

    # Enrichment: join latest pools_dim (batch) to 1m aggregates (stream)
    pools_dim = spark.read.format("delta").load(f"{SILVER}/pools_dim").select(
        F.col("pool_id").alias("pool_id"), "pool_name", "location"
    )

    gold_enr = (
        gold_agg.join(pools_dim, on="pool_id", how="left")
        .withColumn("gold_enriched_ts", F.current_timestamp())
    )

    q_gold_enr = (
        gold_enr.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", gold_enr_chk)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("gold_smartpool_sensors_enriched")
        .start(gold_enr_path)
    )

    # Run for bounded time so Airflow task can finish
    end_time = time.time() + args.run_seconds
    while time.time() < end_time:
        time.sleep(5)

    for q in [q_gold_enr, q_gold_agg, q_silver, q_bronze]:
        try:
            q.stop()
        except Exception:
            logger.exception("Error stopping query %s", q.name)

    spark.stop()

if __name__ == "__main__":
    main()
