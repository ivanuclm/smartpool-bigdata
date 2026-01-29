from confluent_kafka import Producer
import random, json, time, math
from datetime import datetime, timezone

BOOTSTRAP = "192.168.1.40:9094"
TOPIC = "smartpool-sensors"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
})

def delivery_report(err, msg):
    if err:
        print("Entrega fallida:", err)
    else:
        print(f"OK {msg.topic()} [{msg.partition()}] offset={msg.offset()} key={msg.key().decode('utf-8')}")

POOL_IDS = [1, 2, 3, 4, 5, 6]
SLEEP_S = 1

def now_iso_ms():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

for i in range(10_000):
    pool_id = random.choice(POOL_IDS)

    # Valores “normales” (aprox)
    ph = round(random.uniform(7.1, 7.8), 2)
    chlorine = round(random.uniform(0.4, 1.5), 2)
    temp_c = round(random.uniform(18, 30), 2)
    turbidity = round(random.uniform(0.2, 2.0), 2)      # NTU
    water_level = round(random.uniform(70, 100), 2)     # %

    # Anomalías aleatorias para demostrar curación en Silver (5-10%)
    if random.random() < 0.08:
        ph = round(random.uniform(5.5, 9.5), 2)
    if random.random() < 0.08:
        chlorine = round(random.uniform(0.0, 5.0), 2)

    payload = {
        "pool_id": int(pool_id),
        "ts": now_iso_ms(),
        "ph": ph,
        "chlorine_mg_l": chlorine,
        "temp_c": temp_c,
        "turbidity_ntu": turbidity,
        "water_level_pct": water_level,
        # para cruzar con electricidad:
        "pump_kwh_est": round(random.uniform(0.0, 0.6), 3)
    }

    producer.produce(
        TOPIC,
        key=str(pool_id).encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        on_delivery=delivery_report
    )
    producer.poll(0)
    time.sleep(SLEEP_S)

producer.flush(10)
print("Envío completado")
