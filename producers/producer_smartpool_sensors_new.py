
import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger("producer_smartpool_sensors")

def parse_args():
    p = argparse.ArgumentParser()
    # p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--bootstrap", default="192.168.1.40:9094")
    p.add_argument("--topic", default="smartpool-sensors")
    p.add_argument("--rate", type=float, default=10.0, help="messages/sec (approx).")
    p.add_argument("--pools", type=int, default=6)
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()

def make_payload(pool_id: int) -> dict:
    now = datetime.now(timezone.utc).astimezone()  # local tz, keeps offset
    return {
        "pool_id": pool_id,
        "sensor_ts": now.isoformat(timespec="milliseconds"),
        "ph": round(random.uniform(6.8, 8.2), 2),
        "chlorine_mg_l": round(random.uniform(0.2, 2.2), 2),
        "temp_c": round(random.uniform(18.0, 30.0), 2),
        "turbidity_ntu": round(random.uniform(0.0, 2.0), 2),
        "water_level_pct": round(random.uniform(70.0, 100.0), 2),
        "pump_kwh_est": round(random.uniform(0.0, 0.6), 3),
    }

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="1",
        linger_ms=50,
    )

    interval = 1.0 / max(args.rate, 0.1)
    end_time = time.time() + args.seconds

    sent = 0
    try:
        while time.time() < end_time:
            pool_id = random.randint(1, args.pools)
            payload = make_payload(pool_id)
            producer.send(args.topic, key=pool_id, value=payload)
            sent += 1
            time.sleep(interval)
    finally:
        producer.flush(10)
        producer.close()

    logger.info("Sent %s messages to %s", sent, args.topic)

if __name__ == "__main__":
    main()
