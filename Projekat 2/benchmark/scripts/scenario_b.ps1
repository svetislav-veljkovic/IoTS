param(
    [string]$TargetContainer = "iot-ingestion",
    [string]$TargetService = "ingestion",
    [int]$DisconnectSeconds = 30,
    [int]$RecoverSeconds = 30,
    [string]$Network = "iot-network",
    [string]$ResultsDir = "",
    [string]$OutputFile = "scenario_b_results.csv"
)
$ErrorActionPreference = "Continue"

if ([string]::IsNullOrWhiteSpace($ResultsDir)) {
    $ResultsDir = Join-Path $PSScriptRoot "..\results"
}
if (!(Test-Path $ResultsDir)) { New-Item -ItemType Directory -Path $ResultsDir | Out-Null }

function Get-PgCount {
    $r = & docker exec iot-postgres psql -U iotuser -d iot_data -t -c "SELECT COUNT(*) FROM sensor_readings;" 2>&1
    $n = ($r | Where-Object { $_ -match '^\s*\d+' } | Select-Object -First 1) -replace '\s',''
    if ($n -match '^\d+$') { return [long]$n } else { return -1L }
}

function Get-KafkaLag {
    $r = & docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:9092 --describe --group iot-consumers 2>&1
    $total = 0L
    foreach ($line in $r) {
        if ($line -match '^\S.*\s+(\d+)\s+(\d+)\s+(\d+)\s') {
            if ($matches[3] -match '^\d+$') { $total += [long]$matches[3] }
        }
    }
    return $total
}

Write-Host ""
Write-Host "=== Scenario B: Mrezni prekid ($DisconnectSeconds s) ===" -ForegroundColor Yellow
Write-Host "    Kontejner: $TargetContainer"

docker compose restart $TargetService 2>&1 | Out-Null
Start-Sleep -Seconds 5

$logFile = Join-Path $ResultsDir $OutputFile
"Metric,Value,Timestamp" | Out-File -FilePath $logFile -Encoding utf8

Write-Host "[B] Baseline merenje (10s)..." -ForegroundColor Green
Start-Sleep -Seconds 10

$baseline = Get-PgCount
$lagBefore = Get-KafkaLag
"pg_count_baseline,$baseline,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
"kafka_lag_before,$lagBefore,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8

Write-Host "[B] DISCONNECT $TargetContainer ($DisconnectSeconds s)" -ForegroundColor Red
$disconnectTime = Get-Date
docker network disconnect $Network $TargetContainer
"disconnect_start,,$($disconnectTime.ToString('yyyy-MM-dd HH:mm:ss'))" | Out-File -FilePath $logFile -Append -Encoding utf8

Start-Sleep -Seconds $DisconnectSeconds

$countDuringDisconnect = Get-PgCount
"pg_count_during_disconnect,$countDuringDisconnect,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8

Write-Host "[B] RECONNECT $TargetContainer" -ForegroundColor Green
$reconnectTime = Get-Date
docker network connect $Network $TargetContainer
"reconnect_start,,$($reconnectTime.ToString('yyyy-MM-dd HH:mm:ss'))" | Out-File -FilePath $logFile -Append -Encoding utf8


$recoveryDetected = $false
$recoveryStart = Get-Date
$countBeforeRecov = Get-PgCount
$lastCount = $countBeforeRecov

for ($i = 1; $i -le $RecoverSeconds; $i++) {
    Start-Sleep -Seconds 1
    $currentCount = Get-PgCount
    if (-not $recoveryDetected -and $currentCount -gt $lastCount) {
        $recoveryMs = ((Get-Date) - $reconnectTime).TotalMilliseconds
        Write-Host "[B] Recovery detektovan za $([math]::Round($recoveryMs))ms!" -ForegroundColor Green
        "recovery_time_ms,$([math]::Round($recoveryMs)),$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
        $recoveryDetected = $true
    }
    $lastCount = $currentCount
}

$countAfterRecov = Get-PgCount
$lagAfter = Get-KafkaLag

$lost = $countDuringDisconnect - $baseline
$recovered = $countAfterRecov - $countDuringDisconnect

"pg_count_after_recovery,$countAfterRecov,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
"kafka_lag_after,$lagAfter,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
"messages_during_disconnect,$lost,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
"messages_recovered,$recovered,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
if (-not $recoveryDetected) {
    "recovery_time_ms,NOT_DETECTED,$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding utf8
}

Write-Host ""
Write-Host "[B] === REZULTATI ===" -ForegroundColor Green
Write-Host "    Baseline poruka:       $baseline"
Write-Host "    Poruka tokom prekida:  $lost"
Write-Host "    Poruka nakon recovery: $recovered"
Write-Host "    Kafka lag pre:         $lagBefore"
Write-Host "    Kafka lag posle:       $lagAfter"
Write-Host "[INFO] CSV: $logFile" -ForegroundColor Green