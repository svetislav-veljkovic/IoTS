

param(
    [Parameter(Mandatory=$true)][string]$ResultsDir
)
$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$out = Join-Path $ResultsDir "summary.csv"
"Scenario,Broker,QoS_Acks,Devices,Published,PubSucc,PubFail,PubOverrun,LossPercent,AvgThroughput,ConnSucc,ConnFail,E2E_ms,Notes" |
    Out-File -FilePath $out -Encoding utf8
$resourceOut = Join-Path $ResultsDir "resource_summary.csv"
"StatsFile,Samples,AvgCPUPercent,MaxCPUPercent,MaxMemMB,MaxNetRxMB,MaxNetTxMB" |
    Out-File -FilePath $resourceOut -Encoding utf8

function Parse-MqttLog([string]$path, [string]$scenario, [string]$qos, [string]$devs) {
    if (!(Test-Path $path)) { return }
    $content = Get-Content $path -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($content)) {
        "$scenario,MQTT,QoS$qos,$devs,,,,,,,,,," | Out-File -FilePath $out -Append -Encoding utf8
        return
    }
    $pubLines = $content -split "`n" | Where-Object { $_ -match '^(?:\d+m)?\d+s pub total=(\d+) rate=([\d.]+)/sec' }
    $lastPub  = 0; $lastRate = 0.0
    foreach ($l in $pubLines) {
        if ($l -match 'pub total=(\d+) rate=([\d.]+)/sec') {
            $lastPub = [long]$matches[1]; $lastRate = [double]$matches[2]
        }
    }
    $overrunLines = $content -split "`n" | Where-Object { $_ -match 'pub_overrun total=(\d+)' }
    $lastOverrun = 0
    foreach ($l in $overrunLines) {
        if ($l -match 'pub_overrun total=(\d+)') { $lastOverrun = [long]$matches[1] }
    }
    $connSucc = 0
    foreach ($l in ($content -split "`n")) {
        if ($l -match 'connect_succ total=(\d+)') { $connSucc = [long]$matches[1] }
    }
    $rates = @()
    foreach ($l in $pubLines) {
        if ($l -match 'rate=([\d.]+)/sec') { $rates += [double]$matches[1] }
    }
    $avgRate = if ($rates.Count -gt 0) { [math]::Round(($rates | Measure-Object -Average).Average, 1) } else { 0 }

    $loss = "N/A"
    "$scenario,MQTT,QoS$qos,$devs,$lastPub,$lastPub,0,$lastOverrun,$loss,$avgRate,$connSucc,0,,pub_overrun nije delivery loss" |
        Out-File -FilePath $out -Append -Encoding utf8
}

function Parse-KafkaLog([string]$path, [string]$scenario, [string]$acks, [string]$devs) {
    if (!(Test-Path $path)) { return }
    $content = Get-Content $path -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($content)) {
        "$scenario,Kafka,acks=$acks,$devs,,,,,,,,,," | Out-File -FilePath $out -Append -Encoding utf8
        return
    }

    $sent = 0; $throughput = 0.0; $latAvg = 0.0; $lat95 = 0.0
    $recordLines = @($content -split "`n" | Where-Object { $_ -match '^\d+ records sent,' })
    if ($recordLines.Count -gt 0) {
        $last = $recordLines[-1]
        if ($last -match '^(\d+) records sent,\s*([\d.]+) records/sec') {
            $sent = [long]$matches[1]; $throughput = [double]$matches[2]
        }
        if ($last -match '([\d.]+) ms avg latency') { $latAvg = [double]$matches[1] }
        if ($last -match '(\d+) ms 95th') { $lat95 = [double]$matches[1] }
    }
    "$scenario,Kafka,acks=$acks,$devs,$sent,$sent,0,0,0.00,$throughput,,,,$lat95 ms (p95), avg=$latAvg ms" |
        Out-File -FilePath $out -Append -Encoding utf8
}

function Parse-ScenarioDLog([string]$path, [string]$broker, [string]$param) {
    if (!(Test-Path $path)) { return }
    $content = Get-Content $path -Raw -Encoding UTF8
    $e2e = "NOT_FOUND"
    if ($content -match 'E2ELatency=([\d.]+)ms') { $e2e = "$($matches[1])ms" }
    elseif ($content -match 'CRITICAL-ZONE') { $e2e = "FOUND (check log)" }
    "ScenD,$broker,$param,1,,,,,,,,,$e2e," |
        Out-File -FilePath $out -Append -Encoding utf8
}


foreach ($qos in @(0,1,2)) {
    foreach ($devs in @(100,1000,10000)) {
        $log = Join-Path $ResultsDir "mqtt_a_qos${qos}_dev${devs}.log"
        Parse-MqttLog $log "ScenA" $qos $devs
    }
}


foreach ($qos in @(0,1,2)) {
    $log = Join-Path $ResultsDir "mqtt_c_qos${qos}_dev100.log"
    Parse-MqttLog $log "ScenC" $qos 100
}


foreach ($qos in @(0,1,2)) {
    $log = Join-Path $ResultsDir "mqtt_d_qos${qos}.log"
    Parse-ScenarioDLog $log "MQTT" "QoS$qos"
}


foreach ($acks in @("0","1","all")) {
    foreach ($devs in @(100,1000,10000)) {
        $log = Join-Path $ResultsDir "kafka_a_acks${acks}_dev${devs}.log"
        Parse-KafkaLog $log "ScenA" $acks $devs
    }
}


foreach ($acks in @("0","1","all")) {
    $log = Join-Path $ResultsDir "kafka_c_acks${acks}_dev100.log"
    Parse-KafkaLog $log "ScenC" $acks 100
}


foreach ($acks in @("0","1","all")) {
    $log = Join-Path $ResultsDir "kafka_d_acks${acks}.log"
    Parse-ScenarioDLog $log "Kafka" "acks=$acks"
}


Write-Host "[parse] Dohvatam statistiku iz PostgreSQL..."
$pgStats = & docker exec iot-postgres psql -U iotuser -d iot_data -t -c "SELECT broker_type, COUNT(*) FROM sensor_readings GROUP BY broker_type;" 2>&1
"" | Out-File -FilePath $out -Append -Encoding utf8
"# PostgreSQL sensor_readings count:" | Out-File -FilePath $out -Append -Encoding utf8
$pgStats | ForEach-Object { "# $_" | Out-File -FilePath $out -Append -Encoding utf8 }

$pgAlerts = & docker exec iot-postgres psql -U iotuser -d iot_data -t -c "SELECT broker_type, COUNT(*), AVG(avg_e2e_ms) FROM analytics_windows WHERE alert_triggered=true GROUP BY broker_type;" 2>&1
"# Analytics alerts (avg e2e ms):" | Out-File -FilePath $out -Append -Encoding utf8
$pgAlerts | ForEach-Object { "# $_" | Out-File -FilePath $out -Append -Encoding utf8 }

Write-Host "[parse] Agregiram CPU/RAM/network iz *_stats.csv..."
Get-ChildItem $ResultsDir -Filter "*_stats.csv" -ErrorAction SilentlyContinue | ForEach-Object {
    try {
        $rows = Import-Csv $_.FullName | Where-Object {
            $_.Container -and
            $_.Container -notlike "__collector_*__" -and
            $_.CPUPerc -ne "" -and
            $_.MemUsageMB -ne ""
        }
        if ($rows.Count -eq 0) {
            "$($_.Name),0,,,,," | Out-File -FilePath $resourceOut -Append -Encoding utf8
        } else {
            $cpu = @($rows | ForEach-Object { [double]::Parse($_.CPUPerc, [System.Globalization.CultureInfo]::InvariantCulture) })
            $mem = @($rows | ForEach-Object { [double]::Parse($_.MemUsageMB, [System.Globalization.CultureInfo]::InvariantCulture) })
            $rx  = @($rows | ForEach-Object { [double]::Parse($_.NetRxMB, [System.Globalization.CultureInfo]::InvariantCulture) })
            $tx  = @($rows | ForEach-Object { [double]::Parse($_.NetTxMB, [System.Globalization.CultureInfo]::InvariantCulture) })
            $avgCpu = [math]::Round(($cpu | Measure-Object -Average).Average, 2)
            $maxCpu = [math]::Round(($cpu | Measure-Object -Maximum).Maximum, 2)
            $maxMem = [math]::Round(($mem | Measure-Object -Maximum).Maximum, 2)
            $maxRx  = [math]::Round(($rx  | Measure-Object -Maximum).Maximum, 4)
            $maxTx  = [math]::Round(($tx  | Measure-Object -Maximum).Maximum, 4)
            "$($_.Name),$($rows.Count),$avgCpu,$maxCpu,$maxMem,$maxRx,$maxTx" |
                Out-File -FilePath $resourceOut -Append -Encoding utf8
        }
    } catch {
        "$($_.Name),ERROR,,,,," | Out-File -FilePath $resourceOut -Append -Encoding utf8
    }
}

Write-Host "[parse] Gotovo. Summary: $out" -ForegroundColor Green
Write-Host "[parse] Resource summary: $resourceOut" -ForegroundColor Green
