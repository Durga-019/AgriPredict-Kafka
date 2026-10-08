import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RECORDS = os.getenv("TOPIC_RECORDS", "farm-records")
TOPIC_PREDICTIONS = os.getenv("TOPIC_PREDICTIONS", "yield-predictions")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "invalid-records")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "farm_data.csv")
MODEL_PATH = os.path.join(BASE_DIR, "models", "best_model.joblib")
METRICS_PATH = os.path.join(BASE_DIR, "models", "metrics.json")
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "predictions.db"))
PRODUCE_INTERVAL = float(os.getenv("PRODUCE_INTERVAL", "1.0"))

FEATURES = [
    "rainfall_mm", "avg_temp_c", "sunlight_hours", "soil_ph",
    "organic_carbon_pct", "nitrogen_kg_ha", "irrigation", "farm_size_ha",
]
TARGET = "yield_t_ha"

VALID_RANGES = {
    "rainfall_mm": (200, 3000), "avg_temp_c": (10, 45), "sunlight_hours": (2, 14),
    "soil_ph": (3.5, 9.5), "organic_carbon_pct": (0.1, 5), "nitrogen_kg_ha": (0, 300),
    "irrigation": (0, 1), "farm_size_ha": (0.1, 100),
}
