


CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE IF NOT EXISTS sensor_readings (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    device_id       VARCHAR(64) NOT NULL,
    temperature     FLOAT,
    humidity        FLOAT,
    pressure        FLOAT,
    light           INTEGER,
    sound           INTEGER,
    motion          SMALLINT,        
    battery         FLOAT,
    location        VARCHAR(128),
    broker_type     VARCHAR(16),      
    received_at     TIMESTAMPTZ DEFAULT NOW(),  
    ingested_at     TIMESTAMPTZ      
);


CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_readings(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_device_id ON sensor_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_broker ON sensor_readings(broker_type);
CREATE INDEX IF NOT EXISTS idx_sensor_location ON sensor_readings(location);


CREATE TABLE IF NOT EXISTS benchmark_results (
    id              SERIAL PRIMARY KEY,
    scenario        VARCHAR(32) NOT NULL,  
    broker_type     VARCHAR(16) NOT NULL,
    qos_or_acks     VARCHAR(8),           
    device_count    INTEGER,
    msg_per_second  FLOAT,
    total_messages  INTEGER,
    lost_messages   INTEGER,
    throughput_mps  FLOAT,                 
    p50_latency_ms  FLOAT,
    p95_latency_ms  FLOAT,
    p99_latency_ms  FLOAT,
    cpu_percent     FLOAT,
    ram_mb          FLOAT,
    network_rx_mb   FLOAT,
    network_tx_mb   FLOAT,
    test_start      TIMESTAMPTZ,
    test_end        TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS analytics_windows (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    broker_type     VARCHAR(16),
    message_count   INTEGER,
    avg_temperature FLOAT,
    max_temperature FLOAT,
    min_temperature FLOAT,
    avg_humidity    FLOAT,
    alert_triggered BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


CREATE OR REPLACE VIEW v_device_stats AS
SELECT
    device_id,
    broker_type,
    COUNT(*) AS total_readings,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp,
    MIN(temperature) AS min_temp,
    AVG(humidity) AS avg_humidity,
    MAX(received_at) AS last_seen
FROM sensor_readings
GROUP BY device_id, broker_type;


INSERT INTO sensor_readings (
    timestamp, device_id, temperature, humidity, pressure,
    light, sound, motion, battery, location, broker_type
) VALUES (
    NOW(), 'INIT_TEST_DEVICE', 22.5, 55.0, 1013.25,
    300, 45, 0, 98.5, 'test-location', 'init'
);

SELECT 'PostgreSQL inicijalizacija uspesna! Tabele kreirane.' AS status;