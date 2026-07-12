#!/bin/sh
set -eu

API="${EKUIPER_API:-http://ekuiper:9081}"
SOURCE_TOPIC="${MQTT_TOPIC:-iot/sensors}/#"
EVENT_TOPIC="${MQTT_CEP_TOPIC:-iot/analytics/events}"
THRESHOLD="${TEMPERATURE_ALERT_THRESHOLD:-50.0}"
WINDOW_SECONDS="${TUMBLING_WINDOW_SECONDS:-10}"

echo "Waiting for eKuiper REST API at ${API}..."
until curl -fsS "${API}/streams" >/dev/null 2>&1; do
  sleep 2
done

curl -fsS -X DELETE "${API}/rules/iot_cep_alerts" >/dev/null 2>&1 || true
curl -fsS -X DELETE "${API}/streams/iot_sensor_stream" >/dev/null 2>&1 || true

curl -fsS -X POST "${API}/streams" \
  -H "Content-Type: application/json" \
  -d "{\"sql\":\"CREATE STREAM iot_sensor_stream (device_id string, timestamp string, ingested_at float, temperature float, humidity float, pressure float, light bigint, sound bigint, motion bigint, battery float, location string) WITH (TYPE=\\\"mqtt\\\", DATASOURCE=\\\"${SOURCE_TOPIC}\\\", FORMAT=\\\"json\\\", CONF_KEY=\\\"default\\\")\"}"

curl -fsS -X POST "${API}/rules" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\":\"iot_cep_alerts\",
    \"sql\":\"SELECT window_start() AS window_start, window_end() AS window_end, count(*) AS message_count, avg(temperature) AS avg_temperature, max(temperature) AS max_temperature, min(temperature) AS min_temperature, avg(humidity) AS avg_humidity, avg(battery) AS avg_battery, '${THRESHOLD}' AS threshold, 'temperature_window' AS event_type FROM iot_sensor_stream GROUP BY TUMBLINGWINDOW(ss, ${WINDOW_SECONDS}) HAVING avg(temperature) > ${THRESHOLD} OR max(temperature) > ${THRESHOLD}\",
    \"actions\":[
      {\"mqtt\":{\"server\":\"tcp://iot-mosquitto:1883\",\"topic\":\"${EVENT_TOPIC}\",\"qos\":1,\"retained\":false}},
      {\"log\":{}}
    ]
  }"

echo "eKuiper stream and CEP rule are ready. Events topic: ${EVENT_TOPIC}"
