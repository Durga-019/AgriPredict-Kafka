"""Analytics + Orchestration Layer: compare 5 regressors with 5-fold CV, save best model."""
import json
import os
import joblib
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler

from config import DATA_PATH, MODEL_PATH, METRICS_PATH, FEATURES, TARGET
from data_layer import generate_dataset, clean


def models():
    imp = lambda: ("imputer", SimpleImputer(strategy="median"))
    return {
        "Mean Baseline": Pipeline([imp(), ("m", DummyRegressor())]),
        "Linear Regression": Pipeline([imp(), ("m", LinearRegression())]),
        "Polynomial Ridge": Pipeline([imp(), ("p", PolynomialFeatures(2)), ("s", StandardScaler()), ("m", Ridge(1.0))]),
        "Random Forest": Pipeline([imp(), ("m", RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1))]),
        "HistGradientBoosting": Pipeline([imp(), ("m", HistGradientBoostingRegressor(random_state=42))]),
    }


def main():
    if not os.path.exists(DATA_PATH):
        generate_dataset().to_csv(DATA_PATH, index=False)
    df = clean(pd.read_csv(DATA_PATH))
    X, y = df[FEATURES], df[TARGET]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    cv = {}
    for name, pipe in models().items():
        mae = -cross_val_score(pipe, X_tr, y_tr, cv=5, scoring="neg_mean_absolute_error").mean()
        cv[name] = round(float(mae), 4)
        print(f"{name:22s} CV MAE = {mae:.4f}")

    best = min(cv, key=cv.get)
    pipe = models()[best].fit(X_tr, y_tr)
    pred = pipe.predict(X_te)
    pi = permutation_importance(pipe, X_te, y_te, n_repeats=5, random_state=42)
    metrics = {
        "best_model": best, "cv_mae": cv,
        "test": {"mae": float(mean_absolute_error(y_te, pred)),
                 "rmse": float(mean_squared_error(y_te, pred) ** 0.5),
                 "r2": float(r2_score(y_te, pred))},
        "feature_importance": dict(sorted(zip(FEATURES, map(float, pi.importances_mean)), key=lambda t: -t[1])),
        "dataset_profile": df[FEATURES].median().to_dict(),
    }
    joblib.dump(pipe, MODEL_PATH)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nBest model: {best} | Test {metrics['test']}\nSaved -> {MODEL_PATH}")


if __name__ == "__main__":
    main()
