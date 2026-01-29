import pandas as pd
import random
import math
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO
from minio import Minio  # pip install minio

# ====== Config rápida ======
MINIO_ENDPOINT = "localhost:9000"   # si lo ejecutas en tu PC contra MinIO del compose
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "spark"

FOLDER = "landing/electricity_prices"   # recomendado: zona landing
REGION = "ES"

DAYS = 30                 # cuántos CSV (días) generar
LATENCY_MS = 500         # pausa entre ficheros (simula “drop”)
START_DATE_UTC = "2026-01-15"    # None = hoy UTC; o "2026-01-15"

# ====== MinIO client ======
m = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not m.bucket_exists(BUCKET):
    m.make_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' creado.")

def synthetic_daily_prices(date_utc: datetime, region: str = "ES") -> pd.DataFrame:
    # Curva simple: base + seno (picos tarde) + ruido + “finde” más barato
    is_weekend = date_utc.weekday() >= 5
    base = 85.0 if not is_weekend else 70.0  # €/MWh
    rows = []

    for h in range(24):
        # pico aprox 20:00, valle 4:00
        curve = 25.0 * math.sin((h - 14) * math.pi / 12.0)
        noise = random.uniform(-6, 6)
        price_mwh = max(5.0, base + curve + noise)  # no bajar de 5 €/MWh

        ts = datetime(date_utc.year, date_utc.month, date_utc.day, h, 0, 0, tzinfo=timezone.utc)
        rows.append({
            "ts_utc": ts.isoformat().replace("+00:00", "Z"),
            "date": ts.date().isoformat(),
            "hour": h,
            "price_eur_mwh": round(price_mwh, 3),
            "price_eur_kwh": round(price_mwh / 1000.0, 6),
            "region": region,
            "source": "synthetic"
        })

    return pd.DataFrame(rows)

if START_DATE_UTC:
    start = datetime.fromisoformat(START_DATE_UTC).replace(tzinfo=timezone.utc)
else:
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

for i in range(DAYS):
    d = start + timedelta(days=i)
    df = synthetic_daily_prices(d, REGION)

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    object_name = f"{FOLDER}/date={d.date().isoformat()}/prices_{d.date().isoformat()}_{stamp}_{i}.csv"

    m.put_object(
        bucket_name=BUCKET,
        object_name=object_name,
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )

    print(f"CSV subido -> s3a://{BUCKET}/{object_name}")

    if i < DAYS - 1:
        time.sleep(LATENCY_MS / 1000.0)

print("Generación de CSVs completada")