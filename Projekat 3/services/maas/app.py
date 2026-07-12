from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, Field

from train_model import FEATURES, MODEL_NAME, train

MODEL_DIR = Path("model_store")
MODEL_PATH = MODEL_DIR / "model.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"


class WindowFeatures(BaseModel):
    message_count: int = Field(ge=0)
    avg_temperature: float
    max_temperature: float
    min_temperature: float
    avg_humidity: float
    avg_battery: float
    motion_ratio: float
    window_seconds: int = Field(default=10, ge=1)


app = FastAPI(title="IoT MaaS", version="1.0.0")
model = None
metrics: dict = {}


@app.on_event("startup")
def startup() -> None:
    global model, metrics
    if not MODEL_PATH.exists():
        metrics = train(MODEL_DIR)
    else:
        metrics = json.loads(METRICS_PATH.read_text(encoding="utf-8")) if METRICS_PATH.exists() else {}
    model = joblib.load(MODEL_PATH)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model": metrics.get("model_name", MODEL_NAME)}


@app.get("/model/metrics")
def model_metrics() -> dict:
    return metrics


@app.post("/predict/window")
def predict_window(features: WindowFeatures) -> dict:
    row = pd.DataFrame([{name: getattr(features, name) for name in FEATURES}])
    probabilities = model.predict_proba(row)[0]
    classes = list(model.classes_)
    probability_map = {classes[i]: float(probabilities[i]) for i in range(len(classes))}
    risk_level = str(model.predict(row)[0])
    return {
        "model_name": metrics.get("model_name", MODEL_NAME),
        "risk_level": risk_level,
        "risk_score": round(probability_map.get(risk_level, max(probability_map.values())), 4),
        "probabilities": probability_map,
        "features": features.model_dump(),
    }
