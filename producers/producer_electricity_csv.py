
import argparse
import csv
import logging
import os
import random
from datetime import datetime, timedelta

import boto3

logger = logging.getLogger("producer_electricity_csv")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    p.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    p.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
    p.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "spark"))
    p.add_argument("--prefix", default="landing/electricity")
    p.add_argument("--hours", type=int, default=24, help="how many hourly rows to generate")
    p.add_argument("--date", default=None, help="YYYY-MM-DD for the generated day; default today")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    random.seed(args.seed)

    day = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now().date()
    start = datetime(day.year, day.month, day.day, 0, 0, 0)

    rows = []
    for i in range(args.hours):
        ts = start + timedelta(hours=i)
        price = round(random.uniform(30, 180), 2)
        rows.append({"ts": ts.isoformat(sep=" "), "price_eur_mwh": price})

    tmp = f"/tmp/electricity_{day.isoformat()}.csv"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "price_eur_mwh"])
        w.writeheader()
        w.writerows(rows)

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name="us-east-1",
    )

    key = f"{args.prefix}/electricity_{day.isoformat()}.csv"
    s3.upload_file(tmp, args.bucket, key)

    logger.info("Uploaded %s to s3://%s/%s", tmp, args.bucket, key)

if __name__ == "__main__":
    main()
