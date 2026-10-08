"""Kafka Producer: streams live farm sensor records to topic `farm-records`."""
import json
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, TOPIC_RECORDS, PRODUCE_INTERVAL
from data_layer import random_record

FARMS = [f"FARM-{i:03d}" for i in range(1, 21)]


def build_event(rng):
    rec = random_record(rng)
    if rng.random() < 0.05:  # occasional faulty sensor reading
        rec["soil_ph"] = 14.2
    return {"event_id": str(uuid.uuid4()), "farm_id": str(rng.choice(FARMS)),
            "timestamp": datetime.now(timezone.utc).isoformat(), **rec}


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        acks="all", retries=5,
    )
    rng = np.random.default_rng()
    print(f"[producer] streaming to {KAFKA_BOOTSTRAP}/{TOPIC_RECORDS}")
    while True:
        ev = build_event(rng)
        producer.send(TOPIC_RECORDS, key=ev["farm_id"], value=ev)
        print(f"[producer] -> {ev['farm_id']} rain={ev['rainfall_mm']} N={ev['nitrogen_kg_ha']}")
        time.sleep(PRODUCE_INTERVAL)


if __name__ == "__main__":
    main()
