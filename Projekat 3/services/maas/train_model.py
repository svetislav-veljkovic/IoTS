from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

FEATURES = [
    "message_count",
    "avg_temperature",
    "max_temperature",
    "min_temperature",
    "avg_humidity",
    "avg_battery",
    "motion_ratio",
    "window_seconds",
]

MODEL_NAME = "iot-window-risk-random-forest"


def _synthetic_windows(n: int = 4500) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    avg_temp = rng.normal(32, 11, n).clip(5, 82)
    max_temp = avg_temp + rng.normal(7, 5, n).clip(0, 23)
    min_temp = avg_temp - rng.normal(6, 4, n).clip(0, 18)
    avg_humidity = rng.normal(58, 20, n).clip(10, 98)
    avg_battery = rng.normal(62, 24, n).clip(1, 100)
    motion_ratio = rng.beta(2, 5, n)
    message_count = rng.integers(8, 1500, n)
    window_seconds = np.full(n, 10)

    risk_score = (
        0.042 * avg_temp
        + 0.052 * max_temp
        + 0.012 * avg_humidity
        - 0.018 * avg_battery
        + 1.6 * motion_ratio
        + rng.normal(0, 0.45, n)
    )
    labels = np.where(risk_score >= 5.6, "critical", np.where(risk_score >= 4.25, "warning", "normal"))

    return pd.DataFrame(
        {
            "message_count": message_count,
            "avg_temperature": avg_temp,
            "max_temperature": max_temp,
            "min_temperature": min_temp,
            "avg_humidity": avg_humidity,
            "avg_battery": avg_battery,
            "motion_ratio": motion_ratio,
            "window_seconds": window_seconds,
            "risk_level": labels,
        }
    )


def train(output_dir: str | Path = "model_store") -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    dataset_path = os.getenv("MAAS_TRAINING_DATASET")
    if dataset_path and Path(dataset_path).exists():
        raw = pd.read_csv(dataset_path)
        frame = _windows_from_sensor_rows(raw)
        if len(frame) < 150 or frame["risk_level"].nunique() < 2:
            frame = _synthetic_windows()
    else:
        frame = _synthetic_windows()

    x = frame[FEATURES]
    y = frame["risk_level"]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42, stratify=y)

    model = Pipeline(
        [
            ("scale", StandardScaler()),
            ("classifier", RandomForestClassifier(n_estimators=180, max_depth=12, random_state=42, class_weight="balanced")),
        ]
    )
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)
    accuracy = accuracy_score(y_test, predictions)

    metrics = {
        "model_name": MODEL_NAME,
        "features": FEATURES,
        "accuracy": round(float(accuracy), 4),
        "labels": sorted(y.unique().tolist()),
        "classification_report": classification_report(y_test, predictions, output_dict=True),
        "training_rows": int(len(frame)),
    }
    joblib.dump(model, output / "model.joblib")
    (output / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def _windows_from_sensor_rows(raw: pd.DataFrame) -> pd.DataFrame:
    required = {"temperature", "humidity", "battery", "motion"}
    if not required.issubset({c.lower() for c in raw.columns}):
        return _synthetic_windows()

    raw.columns = [c.lower() for c in raw.columns]
    rows = []
    for start in range(0, len(raw), 50):
        chunk = raw.iloc[start : start + 50]
        if chunk.empty:
            continue
        avg_temp = float(chunk["temperature"].mean())
        max_temp = float(chunk["temperature"].max())
        avg_battery = float(chunk["battery"].mean())
        risk = "critical" if avg_temp > 55 or max_temp > 70 else "warning" if avg_temp > 43 or avg_battery < 20 else "normal"
        rows.append(
            {
                "message_count": len(chunk),
                "avg_temperature": avg_temp,
                "max_temperature": max_temp,
                "min_temperature": float(chunk["temperature"].min()),
                "avg_humidity": float(chunk["humidity"].mean()),
                "avg_battery": avg_battery,
                "motion_ratio": float(chunk["motion"].mean()),
                "window_seconds": 10,
                "risk_level": risk,
            }
        )
    return pd.DataFrame(rows) if rows else _synthetic_windows()


if __name__ == "__main__":
    print(json.dumps(train(), indent=2))
