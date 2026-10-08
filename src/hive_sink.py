"""Hive Sink: consumes `yield-predictions` + `invalid-records` from Kafka and
micro-batch inserts them into a Hive table partitioned by (season, yr)."""
import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP, TOPIC_PREDICTIONS, TOPIC_ALERTS, FEATURES, BASE_DIR

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
BATCH_SIZE = int(os.getenv("HIVE_BATCH_SIZE", "25"))
FLUSH_SECONDS = float(os.getenv("HIVE_FLUSH_SECONDS", "30"))
COLS = ["event_id", "farm_id", "event_ts", *FEATURES, "predicted_yield_t_ha", "status"]


def season_of(ts):
    m = datetime.fromisoformat(ts).month
    if 6 <= m <= 10:
        return "Kharif"
    if m >= 11 or m <= 3:
        return "Rabi"
    return "Zaid"


def to_row(ev):
    row = {**ev, "event_ts": ev["timestamp"]}
    row["season"], row["yr"] = season_of(ev["timestamp"]), datetime.fromisoformat(ev["timestamp"]).year
    return row


def sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "\\'") + "'"


def hive_conn():
    from pyhive import hive
    for attempt in range(30):
        try:
            return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username="hive")
        except Exception as e:
            print(f"[hive-sink] waiting for HiveServer2 ({e.__class__.__name__}) ...")
            time.sleep(10)
    raise RuntimeError("HiveServer2 not reachable")


def init_schema(cur):
    lines = open(os.path.join(BASE_DIR, "hive", "ddl.sql")).read().splitlines()
    ddl = "\n".join(l for l in lines if not l.strip().startswith("--"))
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        cur.execute(stmt)
    cur.execute("SET hive.exec.dynamic.partition=true")
    cur.execute("SET hive.exec.dynamic.partition.mode=nonstrict")


def flush(cur, rows):
    values = ",".join("(" + ",".join(sql_val(r.get(c)) for c in COLS + ["season", "yr"]) + ")" for r in rows)
    cur.execute(f"INSERT INTO agripredict.yield_predictions PARTITION (season, yr) VALUES {values}")
    print(f"[hive-sink] flushed {len(rows)} rows to Hive")


def main():
    cur = hive_conn().cursor()
    init_schema(cur)
    consumer = KafkaConsumer(
        TOPIC_PREDICTIONS, TOPIC_ALERTS, bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="agripredict-hive-sink", auto_offset_reset="earliest",
        enable_auto_commit=False, value_deserializer=lambda b: json.loads(b.decode()),
    )
    buf, last = [], time.time()
    print(f"[hive-sink] {TOPIC_PREDICTIONS},{TOPIC_ALERTS} -> hive://{HIVE_HOST}:{HIVE_PORT}")
    while True:
        for msgs in consumer.poll(timeout_ms=1000).values():
            buf.extend(to_row(m.value) for m in msgs)
        if buf and (len(buf) >= BATCH_SIZE or time.time() - last > FLUSH_SECONDS):
            flush(cur, buf)
            consumer.commit()  # at-least-once: commit offsets only after Hive write
            buf, last = [], time.time()


if __name__ == "__main__":
    main()
