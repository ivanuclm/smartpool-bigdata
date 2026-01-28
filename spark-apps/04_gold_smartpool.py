
import argparse
import logging
from pyspark.sql import functions as F

from smartpool_config import create_spark, SILVER, GOLD

logger = logging.getLogger("04_gold_smartpool")

SILVER_POOLS  = f"{SILVER}/pools_dim"
SILVER_EVENTS = f"{SILVER}/maintenance_events"

GOLD_EVENTS_ENR  = f"{GOLD}/maintenance_events_enriched"
GOLD_EVENTS_COST = f"{GOLD}/maintenance_events_cost"

def run(spark):
    pools = spark.read.format("delta").load(SILVER_POOLS)
    events = spark.read.format("delta").load(SILVER_EVENTS)

    # Enriched events (join + simple derived fields)
    enr = (
        events.alias("e")
        .join(pools.select("id", "pool_name", "location").alias("p"), F.col("e.pool_id") == F.col("p.id"), "left")
        .drop(F.col("p.id"))
        .withColumn("gold_calc_ts", F.current_timestamp())
    )

    (
        enr.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_EVENTS_ENR)
    )

    # Cost model placeholder (keep it simple, deterministic)
    cost = (
        enr.withColumn(
            "estimated_cost_eur",
            F.when(F.col("event_type") == F.lit("repair"), F.lit(250.0))
             .when(F.col("event_type") == F.lit("cleaning"), F.lit(80.0))
             .otherwise(F.lit(120.0))
        )
        .withColumn("gold_calc_ts", F.current_timestamp())
    )

    (
        cost.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_EVENTS_COST)
    )

def parse_args():
    p = argparse.ArgumentParser()
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = create_spark("04_gold_smartpool")
    run(spark)
    spark.stop()

if __name__ == "__main__":
    main()
