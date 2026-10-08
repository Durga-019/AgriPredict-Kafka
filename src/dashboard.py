"""Presentation Layer: Streamlit dashboard (live stream, prediction, what-if, insights)."""
import json
import sqlite3
import time

import joblib
import pandas as pd
import streamlit as st

from config import DB_PATH, MODEL_PATH, METRICS_PATH, FEATURES

st.set_page_config(page_title="AgriPredict | Kafka", layout="wide")
st.title("AgriPredict — Real-time Farm Yield Analytics (Kafka)")

model = joblib.load(MODEL_PATH)
metrics = json.load(open(METRICS_PATH))

tab_live, tab_pred, tab_whatif, tab_model, tab_hive = st.tabs(
    ["Live Kafka Stream", "Predict Yield", "What-If", "Model & Insights", "Hive Reports"])

with tab_hive:
    from hive_reports import run_all
    src = st.radio("Source", ["Hive (HiveServer2)", "Local SQL (no Hive)"], horizontal=True)
    if st.button("Run batch reports"):
        try:
            for name, rdf in run_all(local=src.startswith("Local")).items():
                st.subheader(name.replace("_", " ").title())
                st.dataframe(rdf)
                if name == "season_summary" and not rdf.empty:
                    st.bar_chart(rdf.set_index(rdf.columns[0])[rdf.columns[3]])
        except Exception as e:
            st.error(f"Report failed: {e}")

with tab_live:
    try:
        df = pd.read_sql("SELECT * FROM predictions ORDER BY timestamp DESC LIMIT 500",
                         sqlite3.connect(DB_PATH))
    except Exception:
        df = pd.DataFrame()
    if df.empty:
        st.info("No events yet. Start producer + consumer (or simulate.py).")
    else:
        ok = df[df.status == "OK"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Events consumed", len(df))
        c2.metric("Avg predicted yield (t/ha)", f"{ok.predicted_yield.mean():.2f}")
        c3.metric("Invalid records", int((df.status == "INVALID").sum()))
        c4.metric("Active farms", df.farm_id.nunique())
        st.line_chart(ok.sort_values("timestamp").set_index("timestamp")["predicted_yield"])
        st.bar_chart(ok.groupby("farm_id").predicted_yield.mean())
        st.dataframe(df[["timestamp", "farm_id", "predicted_yield", "status"]].head(50))
    auto = st.checkbox("Auto-refresh (3s)", value=True)

def inputs(prefix, defaults):
    cols = st.columns(4)
    vals = {}
    for i, f in enumerate(FEATURES):
        d = float(defaults.get(f, 0))
        if f == "irrigation":
            vals[f] = cols[i % 4].selectbox("irrigation", [0, 1], index=int(round(d)), key=prefix + f)
        else:
            vals[f] = cols[i % 4].number_input(f, value=round(d, 2), key=prefix + f)
    return vals

profile = metrics["dataset_profile"]

with tab_pred:
    v = inputs("p_", profile)
    y = model.predict(pd.DataFrame([v]))[0]
    base = model.predict(pd.DataFrame([profile]))[0]
    st.metric("Predicted yield (t/ha)", f"{y:.2f}", delta=f"{y - base:+.2f} vs dataset median farm")

with tab_whatif:
    feat = st.selectbox("Vary feature", [f for f in FEATURES if f != "irrigation"])
    lo, hi = st.slider("Range", 0.0, 3000.0, (float(profile[feat]) * 0.5, float(profile[feat]) * 1.5))
    grid = pd.DataFrame([{**profile, feat: lo + (hi - lo) * i / 30} for i in range(31)])
    grid["yield"] = model.predict(grid[FEATURES])
    st.line_chart(grid.set_index(feat)["yield"])

with tab_model:
    st.subheader(f"Best model: {metrics['best_model']}")
    st.write(metrics["test"])
    st.bar_chart(pd.Series(metrics["cv_mae"], name="CV MAE"))
    st.bar_chart(pd.Series(metrics["feature_importance"], name="Permutation importance"))

if auto:
    time.sleep(3)
    st.rerun()
