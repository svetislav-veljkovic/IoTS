
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    temperature NUMERIC(5, 2),
    humidity NUMERIC(5, 2),
    co2_level INT,
    voltage NUMERIC(5, 2),
    gps_lat NUMERIC(10, 7),
    gps_lng NUMERIC(10, 7)
);

CREATE INDEX idx_device_time ON sensor_readings (device_id, timestamp DESC);
CREATE INDEX idx_timestamp_brin ON sensor_readings USING BRIN (timestamp);