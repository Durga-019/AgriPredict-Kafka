"""Data Layer: synthetic paddy dataset generation, cleaning and validation."""
import numpy as np
import pandas as pd
from config import FEATURES, TARGET, VALID_RANGES, DATA_PATH


def random_record(rng):
    return {
        "rainfall_mm": round(float(rng.normal(1200, 300)), 1),
        "avg_temp_c": round(float(rng.normal(27, 3)), 1),
        "sunlight_hours": round(float(rng.normal(7, 1.5)), 1),
        "soil_ph": round(float(rng.normal(6.5, 0.6)), 2),
        "organic_carbon_pct": round(float(abs(rng.normal(1.2, 0.4))), 2),
        "nitrogen_kg_ha": round(float(rng.uniform(40, 200)), 1),
        "irrigation": int(rng.integers(0, 2)),
        "farm_size_ha": round(float(rng.uniform(0.5, 20)), 2),
    }


def true_yield(r, rng):
    y = (2.0 + 0.0012 * r["rainfall_mm"] - 0.03 * (r["avg_temp_c"] - 27) ** 2
         + 0.15 * r["sunlight_hours"] - 0.4 * (r["soil_ph"] - 6.5) ** 2
         + 0.5 * r["organic_carbon_pct"] + 0.008 * r["nitrogen_kg_ha"]
         + 0.6 * r["irrigation"] + rng.normal(0, 0.3))
    return round(max(y, 0.5), 3)


def generate_dataset(n=2000, seed=42):
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(n):
        r = random_record(rng)
        r[TARGET] = true_yield(r, rng)
        rows.append(r)
    df = pd.DataFrame(rows)
    # inject missing values and duplicates like a real dataset
    for col in ["soil_ph", "organic_carbon_pct", "sunlight_hours"]:
        df.loc[df.sample(frac=0.03, random_state=seed).index, col] = np.nan
    df = pd.concat([df, df.sample(20, random_state=seed)], ignore_index=True)
    return df


def is_valid(record):
    for k, (lo, hi) in VALID_RANGES.items():
        v = record.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue  # missing allowed; handled by median imputation
        if not (lo <= float(v) <= hi):
            return False
    return True


def clean(df):
    df = df.drop_duplicates()
    mask = df.apply(lambda row: is_valid(row.to_dict()), axis=1)
    df = df[mask & df[TARGET].notna()]
    return df[FEATURES + [TARGET]].reset_index(drop=True)


if __name__ == "__main__":
    d = generate_dataset()
    d.to_csv(DATA_PATH, index=False)
    print(f"Saved {len(d)} rows -> {DATA_PATH}")
