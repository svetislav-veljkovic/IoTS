param(
    [int]$MQTT_QOS = 1,
    [int]$CriticalMessageCount = 30,
    [int]$MaxWaitSec = 35
)
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$mqttDir    = $PSScriptRoot
$benchDir   = Split-Path $mqttDir -Parent
$root       = Split-Path $benchDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "mqtt_d_qos${MQTT_QOS}.log"
$alertTopic = "iot/benchmark-d/qos${MQTT_QOS}"
$publishTopic = "$alertTopic/device-critical"

function Append-Log([string]$Text) {
    [System.IO.File]::AppendAllText($logFile, $Text + "`n", [System.Text.UTF8Encoding]::new($false))
}

Set-Location $root
[System.IO.File]::WriteAllText($logFile, "=== MQTT Scenario D QoS=$MQTT_QOS ===`n", [System.Text.UTF8Encoding]::new($false))
Write-Host "`n=== MQTT Scenario D: izolovan tumbling-window alert (QoS=$MQTT_QOS) ===" -ForegroundColor Yellow

$oldBroker = $env:BROKER_TYPE; $oldTopic = $env:MQTT_TOPIC; $oldStorage = $env:STORAGE_ENABLED; $oldQos = $env:MQTT_QOS
$oldClientId = $env:MQTT_CLIENT_ID; $oldCleanSession = $env:MQTT_CLEAN_SESSION
try {
  
    docker compose stop ingestion storage | Out-Null
    $env:BROKER_TYPE = "mqtt"
    $env:MQTT_TOPIC = $alertTopic
    $env:MQTT_QOS = "$MQTT_QOS"
    $env:MQTT_CLIENT_ID = "analytics-svc-d-qos${MQTT_QOS}"
    $env:MQTT_CLEAN_SESSION = "true"
    $env:STORAGE_ENABLED = "false"
    docker compose up -d --force-recreate analytics | Out-Null
    $ready = $false
    for ($i = 1; $i -le 20; $i++) {
        Start-Sleep -Seconds 1
        $subLine = docker logs iot-analytics 2>&1 |
            Select-String "Analytics \(MQTT\) pretplacen na $([regex]::Escape($alertTopic))/#" |
            Select-Object -Last 1
        if ($subLine) { $ready = $true; Append-Log "READY: $($subLine.ToString())"; break }
    }
    if (-not $ready) { throw "Analytics MQTT subscription nije spreman za topic $alertTopic posle 20s." }

    $containerStart = docker inspect -f '{{.State.StartedAt}}' iot-analytics 2>$null
    Append-Log "AnalyticsStartedAt=$containerStart"
    Append-Log "SubscribeTopic=$alertTopic/#"
    Append-Log "PublishTopic=$publishTopic"

    $sendMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $unixSec = $sendMs / 1000.0
    $unixText = $unixSec.ToString("0.000", [System.Globalization.CultureInfo]::InvariantCulture)
    $msg = '{"device_id":"device-critical","timestamp":"' + [DateTimeOffset]::UtcNow.ToString("o") + '","ingested_at":' + $unixText + ',"temperature":65.0,"humidity":70.0,"pressure":1010.0,"light":0,"sound":0,"motion":1,"battery":90.0,"location":"CRITICAL-ZONE"}'

    Write-Host "[D] Saljem $CriticalMessageCount kriticnih poruka na izolovani topic..." -ForegroundColor Cyan

    $payload = (($msg + "`n") * $CriticalMessageCount)
    $payload | docker exec -i iot-mosquitto mosquitto_pub -h localhost -p 1883 -t $publishTopic -q $MQTT_QOS -l
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
        Write-Host "[D] Alarm pronadjen, ali latencija nije parsirana." -ForegroundColor Yellow
    }
}
catch {
    Append-Log "GRESKA: $($_.Exception.Message)"
    Write-Host "[D] GRESKA: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    if ($null -eq $oldBroker) { Remove-Item Env:BROKER_TYPE -ErrorAction SilentlyContinue } else { $env:BROKER_TYPE = $oldBroker }
    if ($null -eq $oldTopic) { Remove-Item Env:MQTT_TOPIC -ErrorAction SilentlyContinue } else { $env:MQTT_TOPIC = $oldTopic }
    if ($null -eq $oldStorage) { Remove-Item Env:STORAGE_ENABLED -ErrorAction SilentlyContinue } else { $env:STORAGE_ENABLED = $oldStorage }
    if ($null -eq $oldQos) { Remove-Item Env:MQTT_QOS -ErrorAction SilentlyContinue } else { $env:MQTT_QOS = $oldQos }
    if ($null -eq $oldClientId) { Remove-Item Env:MQTT_CLIENT_ID -ErrorAction SilentlyContinue } else { $env:MQTT_CLIENT_ID = $oldClientId }
    if ($null -eq $oldCleanSession) { Remove-Item Env:MQTT_CLEAN_SESSION -ErrorAction SilentlyContinue } else { $env:MQTT_CLEAN_SESSION = $oldCleanSession }
    docker compose up -d --force-recreate storage analytics | Out-Null
}
Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
