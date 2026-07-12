from __future__ import annotations

import os
from datetime import datetime
from html import escape

import psycopg
import requests
from flask import Flask, Response

app = Flask(__name__)

DB = os.getenv(
    "POSTGRES_CONNECTION_STRING",
    "host=iot-postgres port=5432 dbname=iot_data user=iotuser password=iotpassword",
)
MAAS_URL = os.getenv("MAAS_URL", "http://maas:8000").rstrip("/")


def query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg.connect(_psycopg_conninfo(DB)) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())


def _psycopg_conninfo(value: str) -> str:
    if ";" not in value:
        return value
    aliases = {
        "host": "host",
        "port": "port",
        "database": "dbname",
        "username": "user",
        "user id": "user",
        "password": "password",
    }
    parts = []
    for item in value.split(";"):
        if not item.strip() or "=" not in item:
            continue
        key, raw = item.split("=", 1)
        mapped = aliases.get(key.strip().lower())
        if mapped:
            parts.append(f"{mapped}={raw.strip()}")
    return " ".join(parts)


@app.get("/")
def index() -> Response:
    latest_readings = query(
        """
        SELECT received_at, device_id, temperature, humidity, battery, location
        FROM sensor_readings
        ORDER BY timestamp DESC
        LIMIT 8
        """
    )
    windows = query(
        """
        SELECT window_end, message_count, avg_temperature, max_temperature,
               alert_triggered, maas_risk_level, maas_risk_score
        FROM analytics_windows
        ORDER BY window_end DESC
        LIMIT 12
        """
    )
    risk_distribution = query(
        """
        SELECT COALESCE(maas_risk_level, 'pending') AS risk_level, count(*) AS windows
        FROM analytics_windows
        GROUP BY COALESCE(maas_risk_level, 'pending')
        ORDER BY windows DESC
        """
    )
    events = query(
        """
        SELECT received_at, event_type, severity, message_count, avg_temperature, max_temperature
        FROM cep_events
        ORDER BY received_at DESC
        LIMIT 12
        """
    )
    totals = query(
        """
        SELECT
          (SELECT GREATEST(0, reltuples::bigint) FROM pg_class WHERE oid = 'sensor_readings'::regclass) AS readings,
          (SELECT count(*) FROM analytics_windows) AS windows,
          (SELECT count(*) FROM cep_events) AS cep_events,
          (SELECT count(*) FROM analytics_windows WHERE maas_risk_level IS NOT NULL) AS maas_predictions,
          (SELECT max(timestamp) FROM sensor_readings) AS last_reading_at,
          (SELECT max(window_end) FROM analytics_windows) AS last_window_at,
          (SELECT max(received_at) FROM cep_events) AS last_cep_at
        """
    )[0]
    try:
        model = requests.get(f"{MAAS_URL}/model/metrics", timeout=1.5).json()
        maas_ok = True
    except Exception:
        model = {"model_name": "MaaS unavailable", "accuracy": None, "training_rows": None}
        maas_ok = False

    evidence = [
        {"component": "MQTT sensor stream", "evidence": f'{totals["readings"]} stored readings', "status": totals["readings"] > 0},
        {"component": "Analytics tumbling windows", "evidence": f'{totals["windows"]} processed windows', "status": totals["windows"] > 0},
        {"component": "eKuiper CEP", "evidence": f'{totals["cep_events"]} events on iot/analytics/events', "status": totals["cep_events"] > 0},
        {"component": "MaaS REST model", "evidence": f'{totals["maas_predictions"]} ML predictions saved', "status": totals["maas_predictions"] > 0 and maas_ok},
    ]

    html = f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta http-equiv="refresh" content="8">
      <title>IoT Project 3 Dashboard</title>
      <style>
        :root {{ color-scheme: light; --ink:#17212b; --muted:#657381; --line:#d8dee5; --panel:#ffffff; --accent:#0f766e; --warn:#b45309; --bad:#b91c1c; --soft:#edf7f5; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:#f4f7f9; color:var(--ink); }}
        header {{ padding:22px 28px 16px; border-bottom:1px solid var(--line); background:#fff; }}
        h1 {{ margin:0 0 4px; font-size:24px; letter-spacing:0; }}
        h2 {{ margin:0 0 12px; font-size:18px; }}
        main {{ padding:20px 28px 32px; display:grid; gap:18px; }}
        .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; }}
        .two {{ display:grid; grid-template-columns:1.2fr .8fr; gap:14px; }}
        .card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }}
        .metric {{ font-size:28px; font-weight:700; margin-top:6px; }}
        .smallMetric {{ font-size:18px; font-weight:700; margin-top:6px; overflow-wrap:anywhere; }}
        .muted {{ color:var(--muted); font-size:13px; }}
        .pill {{ display:inline-flex; align-items:center; min-height:24px; padding:3px 8px; border-radius:999px; background:var(--soft); color:var(--accent); font-size:12px; font-weight:700; }}
        table {{ width:100%; border-collapse:collapse; font-size:14px; }}
        th,td {{ padding:10px 8px; border-bottom:1px solid var(--line); text-align:left; }}
        th {{ color:var(--muted); font-weight:600; }}
        .ok {{ color:var(--accent); font-weight:700; }}
        .pending {{ color:var(--muted); font-weight:700; }}
        .warning {{ color:var(--warn); font-weight:700; }}
        .critical {{ color:var(--bad); font-weight:700; }}
        @media (max-width: 900px) {{ .grid {{ grid-template-columns:1fr 1fr; }} .two {{ grid-template-columns:1fr; }} }}
        @media (max-width: 620px) {{ header, main {{ padding-left:14px; padding-right:14px; }} .grid {{ grid-template-columns:1fr; }} table {{ font-size:12px; }} }}
      </style>
    </head>
    <body>
      <header>
        <h1>IoT Project 3 Dashboard</h1>
        <div class="muted">MQTT source topic: iot/sensors/# | CEP event topic: iot/analytics/events | refreshed {datetime.utcnow().strftime("%H:%M:%S")} UTC</div>
      </header>
      <main>
        <section class="grid">
          <div class="card"><div class="muted">Sensor readings</div><div class="metric">{totals["readings"]}</div></div>
          <div class="card"><div class="muted">Analytics windows</div><div class="metric">{totals["windows"]}</div></div>
          <div class="card"><div class="muted">CEP events</div><div class="metric">{totals["cep_events"]}</div></div>
          <div class="card"><div class="muted">MaaS predictions</div><div class="metric">{totals["maas_predictions"]}</div></div>
        </section>
        <section class="grid">
          <div class="card"><div class="muted">Last sensor reading</div><div class="smallMetric">{fmt(totals["last_reading_at"])}</div></div>
          <div class="card"><div class="muted">Last analytics window</div><div class="smallMetric">{fmt(totals["last_window_at"])}</div></div>
          <div class="card"><div class="muted">Last CEP event</div><div class="smallMetric">{fmt(totals["last_cep_at"])}</div></div>
          <div class="card"><div class="muted">MaaS model</div><div class="smallMetric">{escape(str(model.get("model_name")))}</div><div class="muted">accuracy={escape(str(model.get("accuracy")))} rows={escape(str(model.get("training_rows")))}</div></div>
        </section>
        <section class="two">
          <div class="card">
            <h2>Project 3 Evidence</h2>
            {table(evidence, ["component","evidence","status"])}
          </div>
          <div class="card">
            <h2>MaaS Risk Distribution</h2>
            {table(risk_distribution, ["risk_level","windows"])}
          </div>
        </section>
        <section class="card">
          <h2>Latest Sensor Readings</h2>
          {table(latest_readings, ["received_at","device_id","temperature","humidity","battery","location"])}
        </section>
        <section class="card">
          <h2>Latest Analytics Windows with MaaS</h2>
          {table(windows, ["window_end","message_count","avg_temperature","max_temperature","alert_triggered","maas_risk_level","maas_risk_score"])}
        </section>
        <section class="card">
          <h2>Latest eKuiper CEP Events</h2>
          {table(events, ["received_at","event_type","severity","message_count","avg_temperature","max_temperature"])}
        </section>
      </main>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


def table(rows: list[dict], columns: list[str]) -> str:
    if not rows:
        return "<p class='muted'>No data yet.</p>"
    head = "".join(f"<th>{c}</th>" for c in columns)
    body = ""
    for row in rows:
        cells = ""
        for col in columns:
            value = row.get(col)
            cls = ""
            if col in {"maas_risk_level", "severity"} and value:
                cls = f" class='{value}'"
            if col == "risk_level" and value:
                cls = f" class='{value}'"
            if col == "status":
                value = "OK" if value else "Pending"
                cls = " class='ok'" if value == "OK" else " class='pending'"
            if isinstance(value, float):
                value = f"{value:.2f}"
            cells += f"<td{cls}>{escape(str(value))}</td>"
        body += f"<tr>{cells}</tr>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def fmt(value) -> str:
    if value is None:
        return "No data yet"
    return escape(str(value))
