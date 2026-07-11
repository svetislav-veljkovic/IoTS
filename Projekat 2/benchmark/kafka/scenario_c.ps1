param(
    [string]$KAFKA_ACKS = "1",
    [int]$DeviceCount = 100,
    [int]$InitialRateTotal = 50,
    [int]$BurstRateTotal = 5000,
    [int]$BurstDurationSeconds = 15,
    [int]$NormalDurationSeconds = 30
)
$ErrorActionPreference = "Continue"
$Network   = "iot-network"
$Bootstrap = "iot-kafka:9092"
$Topic     = "iot-sensors"
$RecSize   = 200

$kafkaDir   = $PSScriptRoot
$benchDir   = Split-Path $kafkaDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "kafka_c_acks$($KAFKA_ACKS)_dev${DeviceCount}.log"
if (Test-Path $logFile) { Remove-Item $logFile -Force }

docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $Topic --partitions 3 --replication-factor 1 2>&1 | Out-Null

$nRec = [int64][math]::Ceiling($InitialRateTotal * $NormalDurationSeconds)
$bRec = [int64][math]::Ceiling($BurstRateTotal * $BurstDurationSeconds)

Write-Host ""
Write-Host "=== Kafka Scenario C: Burst ===" -ForegroundColor Yellow
Write-Host "    acks=$KAFKA_ACKS | init=$InitialRateTotal -> burst=$BurstRateTotal -> init=$InitialRateTotal"
Write-Host ""
function Run($tps, $rec) {
    & docker run --rm --network $Network apache/kafka:3.7.0 /opt/kafka/bin/kafka-producer-perf-test.sh `
        --topic $Topic --num-records $rec --record-size $RecSize --throughput $tps `
        --producer-props bootstrap.servers=$Bootstrap acks=$KAFKA_ACKS linger.ms=5 batch.size=65536 compression.type=lz4 2>&1
}
Write-Host "[FAZA 1] Normal $NormalDurationSeconds s..." -ForegroundColor Cyan
Run $InitialRateTotal $nRec | Out-Null
Write-Host "[FAZA 2] BURST $BurstDurationSeconds s..." -ForegroundColor Red
Run $BurstRateTotal $bRec | Tee-Object -FilePath $logFile
Write-Host "[FAZA 3] Recovery $NormalDurationSeconds s..." -ForegroundColor Cyan
Run $InitialRateTotal $nRec | Out-Null
Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
