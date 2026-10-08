import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

fig, ax = plt.subplots(figsize=(16, 9), dpi=150)
ax.set_xlim(0, 16); ax.set_ylim(0, 9); ax.axis("off")
C = {"src": "#E8EEF4", "kafka": "#1F4E79", "ml": "#2E7D32", "hive": "#F2B705", "ui": "#6A1B9A", "db": "#607D8B"}


def box(x, y, w, h, title, sub, color, tc="white"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05,rounding_size=0.15", fc=color, ec="#222", lw=1.2))
    ax.text(x + w / 2, y + h - 0.32, title, ha="center", va="top", fontsize=11.5, weight="bold", color=tc)
    ax.text(x + w / 2, y + h / 2 - 0.25, sub, ha="center", va="center", fontsize=8.8, color=tc, linespacing=1.4)


def arrow(x1, y1, x2, y2, label=""):
    ax.annotate("", (x2, y2), (x1, y1), arrowprops=dict(arrowstyle="-|>", lw=1.6, color="#333"))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.15, label, ha="center", fontsize=8, style="italic", color="#333")


ax.text(8, 8.65, "AgriPredict — Kafka + Hive Architecture (Lambda)", ha="center", fontsize=17, weight="bold")
ax.add_patch(FancyBboxPatch((0.2, 4.35), 15.6, 3.85, boxstyle="round,pad=0.02", fc="#F4F8FC", ec="#9DB3C8", ls="--"))
ax.text(0.4, 7.95, "SPEED LAYER (real-time)", fontsize=9, weight="bold", color="#1F4E79")
ax.add_patch(FancyBboxPatch((0.2, 0.25), 15.6, 3.85, boxstyle="round,pad=0.02", fc="#FFFBEA", ec="#D9B54A", ls="--"))
ax.text(0.4, 0.4, "BATCH LAYER (Hive)", fontsize=9, weight="bold", color="#8A6D00")

box(0.5, 5.3, 2.4, 2.0, "Farm Sensors", "producer.py\n20 farms, JSON events\nkey = farm_id", C["src"], "#111")
box(3.6, 4.7, 3.0, 3.2, "Apache Kafka", "farm-records (3 partitions)\n\nyield-predictions\n\ninvalid-records", C["kafka"])
box(7.4, 5.3, 3.0, 2.0, "Stream Processor", "consumer.py\nvalidate → median impute\n→ ML predict (t/ha)", C["ml"])
box(7.4, 2.35, 3.0, 1.45, "ML Training", "train.py · 5 models\n5-fold CV · best_model.joblib", C["ml"])
box(11.2, 5.5, 2.0, 1.6, "SQLite Store", "predictions.db", C["db"])
box(13.6, 3.2, 2.1, 2.6, "Dashboard", "Streamlit\nLive · Predict\nWhat-If · Insights\nHive Reports", C["ui"])
box(3.6, 1.0, 3.0, 2.0, "Hive Sink", "hive_sink.py\nmicro-batch inserts\nat-least-once commit", C["hive"], "#111")
box(11.0, 0.7, 2.4, 2.9, "Apache Hive", "agripredict.\nyield_predictions\nORC · PARTITIONED BY\n(season, yr)\n+ HiveQL reports", C["hive"], "#111")

arrow(2.9, 6.3, 3.6, 6.3, "publish")
arrow(6.6, 6.8, 7.4, 6.6, "consume")
arrow(7.4, 5.8, 6.6, 5.6, "publish")
arrow(8.9, 3.8, 8.9, 5.3, "model")
arrow(10.4, 6.3, 11.2, 6.3, "persist")
arrow(13.2, 6.0, 13.6, 5.2)
arrow(5.1, 4.7, 5.1, 3.0, "consume predictions")
arrow(6.6, 2.0, 11.0, 2.0, "INSERT ... PARTITION (season, yr)")
arrow(13.4, 2.4, 13.6, 3.4, "SQL reports")

plt.savefig(__file__.replace("make_diagram.py", "architecture.png"), bbox_inches="tight", facecolor="white")
