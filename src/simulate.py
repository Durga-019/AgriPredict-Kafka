"""Offline demo: same pipeline WITHOUT a Kafka broker (in-memory queue simulates the topic)."""
import queue
import threading
import time

import joblib
import numpy as np

from config import MODEL_PATH, PRODUCE_INTERVAL
from producer import build_event
from consumer import process
import storage

topic = queue.Queue()


def produce():
    rng = np.random.default_rng()
    while True:
        topic.put(build_event(rng))
        time.sleep(PRODUCE_INTERVAL)


def consume():
    model, db = joblib.load(MODEL_PATH), storage.conn()
    while True:
        ev = topic.get()
        pred, status = process(ev, model)
        storage.save(db, ev, pred, status)
        print(f"[sim] {ev['farm_id']} -> {status} {pred}")


if __name__ == "__main__":
    threading.Thread(target=produce, daemon=True).start()
    consume()
