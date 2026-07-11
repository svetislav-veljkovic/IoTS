param(
    [string]$KAFKA_ACKS = "1",
    [int]$CriticalMessageCount = 30,
    [int]$MaxWaitSec = 35
)
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$kafkaDir   = $PSScriptRoot
$benchDir   = Split-Path $kafkaDir -Parent
$root       = Split-Path $benchDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "kafka_d_acks$($KAFKA_ACKS).log"
$topicSafe = $KAFKA_ACKS -replace '[^a-zA-Z0-9]','-'
$alertTopic = "iot-alerts-d-$topicSafe"

function Append-Log([string]$Text) {
    [System.IO.File]::AppendAllText($logFile, $Text + "`n", [System.Text.UTF8Encoding]::new($false))
}

Set-Location $root
[System.IO.File]::WriteAllText($logFile, "=== Kafka Scenario D acks=$KAFKA_ACKS ===`n", [System.Text.UTF8Encoding]::new($false))
Write-Host "`n=== Kafka Scenario D: izolovan tumbling-window alert (acks=$KAFKA_ACKS) ===" -ForegroundColor Yellow

$oldBroker = $env:BROKER_TYPE; $oldTopic = $env:KAFKA_TOPIC; $oldStorage = $env:STORAGE_ENABLED; $oldAcks = $env:KAFKA_ACKS
try {
    docker compose stop ingestion storage | Out-Null
    docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic $alertTopic 2>$null | Out-Null
    Start-Sleep -Seconds 1
    docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $alertTopic --partitions 1 --replication-factor 1 | Out-Null

    $env:BROKER_TYPE = "kafka"
    $env:KAFKA_TOPIC = $alertTopic
    $env:KAFKA_ACKS = $KAFKA_ACKS
    $env:STORAGE_ENABLED = "false"
    docker compose up -d --force-recreate analytics | Out-Null
    $ready = $false
    for ($i = 1; $i -le 25; $i++) {
        Start-Sleep -Seconds 1
        $subLine = docker logs iot-analytics 2>&1 |
            Select-String "Analytics \(Kafka\): .*topic=$([regex]::Escape($alertTopic))" |
            Select-Object -Last 1
        if ($subLine) { $ready = $true; Append-Log "READY: $($subLine.ToString())"; break }
    }
    if (-not $ready) { throw "Analytics Kafka consumer nije spreman za topic $alertTopic posle 25s." }

    $containerStart = docker inspect -f '{{.State.StartedAt}}' iot-analytics 2>$null
    Append-Log "AnalyticsStartedAt=$containerStart"
    Append-Log "Topic=$alertTopic"

    $sendMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $unixSec = $sendMs / 1000.0
    $unixText = $unixSec.ToString("0.000", [System.Globalization.CultureInfo]::InvariantCulture)
    $msg = '{"device_id":"device-critical","timestamp":"' + [DateTimeOffset]::UtcNow.ToString("o") + '","ingested_at":' + $unixText + ',"temperature":65.0,"humidity":70.0,"pressure":1010.0,"light":0,"sound":0,"motion":1,"battery":90.0,"location":"CRITICAL-ZONE"}'

    Write-Host "[D] Saljem $CriticalMessageCount kriticnih poruka na izolovani topic..." -ForegroundColor Cyan
    $payload = (($msg + "`n") * $CriticalMessageCount)
    $payload | docker exec -i iot-kafka `
        /opt/kafka/bin/kafka-console-producer.sh `
        --bootstrap-server localhost:9092 --topic $alertTopic `
        --producer-property acks=$KAFKA_ACKS 2>&1 | ForEach-Object { Append-Log $_ }
    Append-Log "SentAtUnixMs=$sendMs"
    Append-Log "Published=$CriticalMessageCount"

    Write-Host "[D] Cekam zatvaranje 10-sekundnog prozora (do ${MaxWaitSec}s)..." -ForegroundColor Cyan
    $elapsed = 0; $found = $null
    while ($elapsed -lt $MaxWaitSec) {
        Start-Sleep -Seconds 1; $elapsed++
        $found = docker logs iot-analytics 2>&1 |
            Select-String 'CRITICAL ALERT \(window avg\).*CRITICAL-ZONE' |
            Select-Object -Last 1
        if ($found) { break }
    }

    if (-not $found) { throw "Tumbling-window alarm nije pronadjen za ${MaxWaitSec}s." }
    $text = $found.ToString()
    Append-Log "FOUND: $text"
    if ($text -match 'E2ELatency=([\d.]+)ms') {
        Append-Log "E2ELatency=$($matches[1])ms"
        Write-Host "[D] E2E latencija tumbling prozora: $($matches[1])ms" -ForegroundColor Green
    } else {
        Append-Log "E2ELatency=PARSE_ERROR"
    }
}
catch {
    Append-Log "GRESKA: $($_.Exception.Message)"
    Write-Host "[D] GRESKA: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    if ($null -eq $oldBroker) { Remove-Item Env:BROKER_TYPE -ErrorAction SilentlyContinue } else { $env:BROKER_TYPE = $oldBroker }
    if ($null -eq $oldTopic) { Remove-Item Env:KAFKA_TOPIC -ErrorAction SilentlyContinue } else { $env:KAFKA_TOPIC = $oldTopic }
    if ($null -eq $oldStorage) { Remove-Item Env:STORAGE_ENABLED -ErrorAction SilentlyContinue } else { $env:STORAGE_ENABLED = $oldStorage }
    if ($null -eq $oldAcks) { Remove-Item Env:KAFKA_ACKS -ErrorAction SilentlyContinue } else { $env:KAFKA_ACKS = $oldAcks }
    docker compose up -d --force-recreate storage analytics | Out-Null
}
Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
