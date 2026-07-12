CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS sensor_readings (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    device_id       VARCHAR(64) NOT NULL,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    pressure        DOUBLE PRECISION,
    light           INTEGER,
    sound           INTEGER,
    motion          SMALLINT,
    battery         DOUBLE PRECISION,
    location        VARCHAR(128),
    broker_type     VARCHAR(16),
    ingested_at     DOUBLE PRECISION,
    received_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_readings(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_device_id ON sensor_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_broker    ON sensor_readings(broker_type);

CREATE TABLE IF NOT EXISTS analytics_windows (
    id               SERIAL PRIMARY KEY,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    broker_type      VARCHAR(16),
    message_count    INTEGER,
    avg_temperature  DOUBLE PRECISION,
    max_temperature  DOUBLE PRECISION,
    min_temperature  DOUBLE PRECISION,
    avg_humidity     DOUBLE PRECISION,
    alert_triggered  BOOLEAN DEFAULT FALSE,
    avg_e2e_ms       DOUBLE PRECISION,
    maas_risk_level   VARCHAR(32),
    maas_risk_score   DOUBLE PRECISION,
    maas_model_name   VARCHAR(128),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cep_events (
    id               SERIAL PRIMARY KEY,
    event_type       VARCHAR(64) NOT NULL,
    source           VARCHAR(64) NOT NULL DEFAULT 'ekuiper',
    window_start     TIMESTAMPTZ,
    window_end       TIMESTAMPTZ,
    message_count    INTEGER,
    avg_temperature  DOUBLE PRECISION,
    max_temperature  DOUBLE PRECISION,
    avg_humidity     DOUBLE PRECISION,
    severity         VARCHAR(32),
    payload          JSONB NOT NULL,
    received_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cep_events_received ON cep_events(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_cep_events_type ON cep_events(event_type);

INSERT INTO sensor_readings (timestamp, device_id, temperature, humidity, pressure, light, sound, motion, battery, location, broker_type)
VALUES (NOW(), 'INIT_TEST_DEVICE', 22.5, 55.0, 1013.25, 300, 45, 0, 98.5, 'test-location', 'init');
