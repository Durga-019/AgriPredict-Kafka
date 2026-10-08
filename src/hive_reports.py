"""Runs the HiveQL batch reports. Use --local to run the same SQL on the SQLite store (no Hive)."""
import json
import os
import sqlite3
import sys

import pandas as pd

from config import BASE_DIR, DB_PATH, FEATURES
from hive_sink import season_of

REPORTS_FILE = os.path.join(BASE_DIR, "hive", "reports.sql")


def load_reports():
    reports, name = {}, None
    for line in open(REPORTS_FILE):
        if line.startswith("-- name:"):
            name = line.split(":", 1)[1].strip()
            reports[name] = ""
        elif name:
            reports[name] += line
    return {k: v.strip().rstrip(";") for k, v in reports.items()}


def local_conn():
    """Builds an in-memory table mirroring the Hive schema from the SQLite stream store."""
    src = pd.read_sql("SELECT payload, predicted_yield, status FROM predictions", sqlite3.connect(DB_PATH))
    rows = []
    for p, y, s in src.itertuples(index=False):
        ev = json.loads(p)
        rows.append({"farm_id": ev["farm_id"], **{f: ev.get(f) for f in FEATURES},
                     "predicted_yield_t_ha": y, "status": s, "season": season_of(ev["timestamp"])})
    mem = sqlite3.connect(":memory:")
    mem.execute("ATTACH ':memory:' AS agripredict")
    pd.DataFrame(rows).to_sql("tmp", mem, index=False)
    mem.execute("CREATE TABLE agripredict.yield_predictions AS SELECT * FROM main.tmp")
    return mem


def hive_conn():
    from pyhive import hive
    return hive.Connection(host=os.getenv("HIVE_HOST", "localhost"),
                           port=int(os.getenv("HIVE_PORT", "10000")), username="hive")


def run_all(local=False):
    conn = local_conn() if local else hive_conn()
    return {name: pd.read_sql(sql, conn) for name, sql in load_reports().items()}


if __name__ == "__main__":
    for name, df in run_all(local="--local" in sys.argv).items():
        print(f"\n=== {name} ===\n{df.to_string(index=False)}")
