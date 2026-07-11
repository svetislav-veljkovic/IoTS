param(
    [string]$KAFKA_ACKS = "1",
    [int]$DeviceCount = 100,
    [int]$MsgRatePerDevice = 10,
    [int]$DurationSeconds = 60
)
$ErrorActionPreference = "Continue"

$Network   = "iot-network"
$Bootstrap = "iot-kafka:9092"
$Topic     = "iot-sensors"
$TotalTps  = $DeviceCount * $MsgRatePerDevice
$Records   = [int64][math]::Ceiling($TotalTps * $DurationSeconds)
$RecSize   = 200

$kafkaDir   = $PSScriptRoot
$benchDir   = Split-Path $kafkaDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "kafka_a_acks$($KAFKA_ACKS)_dev${DeviceCount}.log"
if (Test-Path $logFile) { Remove-Item $logFile -Force }

docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $Topic --partitions 3 --replication-factor 1 2>&1 | Out-Null

Write-Host ""
Write-Host "=== Kafka Scenario A: Massive Sensor Ingestion ===" -ForegroundColor Yellow
Write-Host "    acks          = $KAFKA_ACKS"
Write-Host "    Uredjaja      = $DeviceCount"
Write-Host "    Rate/device   = $MsgRatePerDevice msg/s"
Write-Host "    Ukupan target = ~$TotalTps msg/s"
Write-Host "    Rekorda       = $Records"
Write-Host ""

& docker run --rm --network $Network apache/kafka:3.7.0 `
    /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic $Topic --num-records $Records --record-size $RecSize --throughput $TotalTps `
    --producer-props bootstrap.servers=$Bootstrap acks=$KAFKA_ACKS linger.ms=5 batch.size=65536 compression.type=lz4 `
    2>&1 | Tee-Object -FilePath $logFile
Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
