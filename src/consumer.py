"""Kafka Consumer (stream processor): validates records, predicts yield,
publishes to `yield-predictions` / `invalid-records`, and persists to SQLite."""
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from config import (KAFKA_BOOTSTRAP, TOPIC_RECORDS, TOPIC_PREDICTIONS,
                    TOPIC_ALERTS, MODEL_PATH, FEATURES)
from data_layer import is_valid
import storage


def process(event, model):
    if not is_valid(event):
        return None, "INVALID"
    X = pd.DataFrame([{f: event.get(f) for f in FEATURES}])
    return round(float(model.predict(X)[0]), 3), "OK"


def main():
    model = joblib.load(MODEL_PATH)
    consumer = KafkaConsumer(
        TOPIC_RECORDS, bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="agripredict-predictor", auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v).encode())
    db = storage.conn()
    print(f"[consumer] listening on {TOPIC_RECORDS}")
    for msg in consumer:
        ev = msg.value
        pred, status = process(ev, model)
        storage.save(db, ev, pred, status)
        out = {**ev, "predicted_yield_t_ha": pred, "status": status}
        producer.send(TOPIC_PREDICTIONS if status == "OK" else TOPIC_ALERTS, out)
        print(f"[consumer] {ev['farm_id']} p{msg.partition}@{msg.offset} -> {status} {pred}")


if __name__ == "__main__":
    main()
