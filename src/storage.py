import json
import sqlite3
from config import DB_PATH


def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("""CREATE TABLE IF NOT EXISTS predictions(
        event_id TEXT PRIMARY KEY, farm_id TEXT, timestamp TEXT,
        payload TEXT, predicted_yield REAL, status TEXT)""")
    return c


def save(c, event, pred, status):
    c.execute("INSERT OR REPLACE INTO predictions VALUES (?,?,?,?,?,?)",
              (event["event_id"], event["farm_id"], event["timestamp"],
               json.dumps(event), pred, status))
    c.commit()
