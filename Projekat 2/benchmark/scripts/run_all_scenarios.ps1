
param(
    [ValidateSet("Quick","Standard","Full")]
    [string]$Mode = "Standard",
    [switch]$QuickMode,   
    [switch]$AppendResults
)

$ErrorActionPreference = "Continue"
if ($QuickMode) { $Mode = "Quick" }

$scriptsDir = $PSScriptRoot
$benchDir   = Split-Path $scriptsDir -Parent
$root       = Split-Path $benchDir -Parent
Set-Location $root

$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
if (-not $AppendResults) {
    Get-ChildItem $resultsDir -File -Filter *.csv -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
    Get-ChildItem $resultsDir -File -Filter *.log -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
}

$collectScript = Join-Path $scriptsDir "collect_stats.ps1"
$lagScript     = Join-Path $scriptsDir "kafka_lag_check.ps1"
$mqttA  = Join-Path $benchDir "mqtt\scenario_a.ps1"
$mqttC  = Join-Path $benchDir "mqtt\scenario_c.ps1"
$mqttD  = Join-Path $benchDir "mqtt\scenario_d.ps1"
$kafkaA = Join-Path $benchDir "kafka\scenario_a.ps1"
$kafkaC = Join-Path $benchDir "kafka\scenario_c.ps1"
$kafkaD = Join-Path $benchDir "kafka\scenario_d.ps1"
$scenB  = Join-Path $scriptsDir "scenario_b.ps1"

switch ($Mode) {
    "Quick" {
        $ADuration = 4; $CNormalDur = 2; $CBurstDur = 1
        $DisconnectSec = 3; $RecoverSec = 3
        $PauseSeconds = 1; $BrokerWait = 3; $DMaxWait = 25
    }
    "Standard" {
        $ADuration = 45; $CNormalDur = 20; $CBurstDur = 15
        $DisconnectSec = 30; $RecoverSec = 30
        $PauseSeconds = 2; $BrokerWait = 12; $DMaxWait = 60
    }
    "Full" {
        $ADuration = 60; $CNormalDur = 30; $CBurstDur = 15
        $DisconnectSec = 30; $RecoverSec = 30
        $PauseSeconds = 2; $BrokerWait = 15; $DMaxWait = 60
    }
}
$DeviceCounts = if ($Mode -eq "Quick") { @(100) } else { @(100, 1000, 10000) }
Write-Host "`n[MODE] $Mode" -ForegroundColor Yellow

function Get-StatsTimeoutMs([int]$devs) {
  
   
    if ($devs -ge 5000) { return 20000 }
    elseif ($devs -ge 1000) { return 15000 }
    else { return 12000 }
}

function Get-CooldownSeconds([int]$devs) {
    if ($devs -ge 5000) { return 15 }
    elseif ($devs -ge 1000) { return 6 }
    else { return 2 }
}

function Start-StatsJob([string]$leaf, [int]$DeviceCountForTimeout = 0) {
    $timeoutMs = Get-StatsTimeoutMs $DeviceCountForTimeout
    $args = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $collectScript,
        "-ResultsDir", $resultsDir,
        "-OutputFile", $leaf,
        "-TimeoutMs", "$timeoutMs",
        "-IntervalMs", "1000"
    )
    $p = Start-Process -FilePath "powershell.exe" -ArgumentList $args -PassThru -WindowStyle Hidden
    Write-Host "[run] stats pid=$($p.Id) file=$leaf timeoutMs=$timeoutMs" -ForegroundColor DarkCyan
    $f = Join-Path $resultsDir $leaf
    $dead = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $dead) {
        if (Test-Path $f) {
            $c = Get-Content $f -ErrorAction SilentlyContinue
            if ($c -match ",(docker-stats-batch|docker-stats-single|cgroup-exec),") { return $p }
        }
        Start-Sleep -Milliseconds 300
    }
    Write-Host "[run] UPOZORENJE: Stats fajl nije dobio prvi metricki uzorak za $leaf" -ForegroundColor Yellow
    return $p
}
function Stop-JobSafe($j) {
    if ($null -eq $j) { return }
    try {
        if ($j -is [System.Diagnostics.Process]) {
            if (-not $j.HasExited) { Stop-Process -Id $j.Id -Force -ErrorAction SilentlyContinue }
            $j.Dispose()
        } else {
            Stop-Job $j -ErrorAction SilentlyContinue
            Remove-Job $j -Force -ErrorAction SilentlyContinue
        }
    } catch { }
}
function Start-LagJob([string]$leaf, [int]$durSec) {
    $j = Start-Job -ScriptBlock { param($s,$d,$f,$g,$dur) & $s -ResultsDir $d -OutputFile $f -GroupId $g -DurationSeconds $dur } `
                   -ArgumentList $lagScript, $resultsDir, $leaf, "iot-consumers", $durSec
    Write-Host "[run] lag job=$($j.Id) file=$leaf dur=${durSec}s" -ForegroundColor DarkCyan
    return $j
}
function Assert-File($leaf) {
    $f = Join-Path $resultsDir $leaf
    if (Test-Path $f) {
        $lines = (Get-Content $f | Measure-Object -Line).Lines
        $size  = (Get-Item $f).Length
        if ($leaf -like "*_stats.csv") {
            $hasMetric = Select-String -Path $f -Pattern ',(cgroup-exec|docker-stats-batch|docker-stats-single),' -Quiet -ErrorAction SilentlyContinue
            $ok = $hasMetric -and $lines -gt 3 -and $size -gt 80
        } elseif ($leaf -like "*_lag.csv") {
            $ok = $lines -gt 1 -and $size -gt 85
        } else {
            $ok = $lines -gt 1 -and $size -gt 50
        }
        $col = if ($ok) { "Green" } else { "Red" }
        Write-Host ("[verify] {0,-55} {1,6} lines, {2,8} B" -f $leaf, $lines, $size) -ForegroundColor $col
        if ($col -eq "Red") {
            Write-Host "[verify] UPOZORENJE: Fajl nema dovoljno validnih redova za ovaj tip rezultata." -ForegroundColor Yellow
        }
    } else {
        Write-Host "[verify] NE POSTOJI: $leaf" -ForegroundColor Red
    }
}
function Switch-Broker([string]$Type) {
    Write-Host "`n[broker] Prebacujem BROKER_TYPE=$Type za benchmark konzumente..." -ForegroundColor Magenta
    $env:BROKER_TYPE = $Type
    $env:STORAGE_ENABLED = "false"
  
    docker compose stop ingestion 2>&1 | Out-Null
    docker compose up -d --build --force-recreate storage analytics | Out-Null
    Start-Sleep -Seconds $BrokerWait
}
function Switch-BrokerForScenarioB([string]$Type) {
    $env:BROKER_TYPE = $Type
    $env:STORAGE_ENABLED = "true"
    docker compose up -d --build --force-recreate storage analytics ingestion | Out-Null
    Start-Sleep -Seconds $BrokerWait
}
function Reset-KafkaTopic {
    docker compose stop storage analytics 2>&1 | Out-Null
    docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic iot-sensors 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic iot-sensors --partitions 3 --replication-factor 1 2>&1 | Out-Null
    docker compose up -d --force-recreate storage analytics | Out-Null
    Start-Sleep -Seconds 5
}
function Run-Step {
    # Bezbedno izvrsavanje jednog scenario koraka - jedan izuzetak/pad ovde vise ne obara ceo run.
    param([scriptblock]$Block, [string]$Name)
    try { & $Block } catch { Write-Host "[run] GRESKA u koraku '$Name': $($_.Exception.Message) - nastavljam." -ForegroundColor Red }
}

$swTotal = [System.Diagnostics.Stopwatch]::StartNew()

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "  IoT Projekat 2 - BENCHMARK TESTOVI ($Mode mod)" -ForegroundColor Green
Write-Host "  Radni dir: $root" -ForegroundColor Green
Write-Host "  Results:   $resultsDir" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green

docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic iot-sensors --partitions 3 --replication-factor 1 2>&1 | Out-Null

Switch-Broker "mqtt"

Write-Host ""
Write-Host "--- MQTT ---" -ForegroundColor Cyan
$mqttQosLevels = @(0,1,2)
foreach ($qos in $mqttQosLevels) {

    $devsForThisQos = if ($Mode -eq "Full") { $DeviceCounts } else { if ($qos -eq 1) { $DeviceCounts } else { @(100) } }

    foreach ($devs in $devsForThisQos) {
        $leaf = "mqtt_a_qos${qos}_dev${devs}_stats.csv"
        Write-Host "`n>>> MQTT QoS=$qos devices=$devs (~${ADuration}s) [proteklo: $([int]$swTotal.Elapsed.TotalMinutes) min]" -ForegroundColor Yellow
        if ($devs -ge 5000) {
            Write-Host "[run] NAPOMENA: $devs paralelnih konekcija moze znacajno opteretiti Docker Desktop." -ForegroundColor DarkYellow
        }
        Run-Step -Name "mqtt-a-$qos-$devs" -Block {
            $j = Start-StatsJob $leaf $devs
            Start-Sleep -Seconds $PauseSeconds
            & powershell -NoProfile -ExecutionPolicy Bypass -File $mqttA -MQTT_QOS $qos -DeviceCount $devs -DurationSeconds $ADuration
            Start-Sleep -Seconds $PauseSeconds
            Stop-JobSafe $j
            Assert-File $leaf
        }
        Start-Sleep -Seconds (Get-CooldownSeconds $devs)
    }

    $leaf = "mqtt_c_qos${qos}_dev100_stats.csv"
    Write-Host "`n>>> MQTT QoS=$qos (Scenario C, burst)" -ForegroundColor Yellow
    Run-Step -Name "mqtt-c-$qos" -Block {
        $j = Start-StatsJob $leaf 100
        Start-Sleep -Seconds $PauseSeconds
        & powershell -NoProfile -ExecutionPolicy Bypass -File $mqttC -MQTT_QOS $qos -DeviceCount 100 -InitialRateTotal 50 -BurstRateTotal 5000 -BurstDurationSeconds $CBurstDur -NormalDurationSeconds $CNormalDur
        Start-Sleep -Seconds $PauseSeconds
        Stop-JobSafe $j
        Assert-File $leaf
    }
    Start-Sleep -Seconds $PauseSeconds

    Write-Host "`n>>> MQTT QoS=$qos (Scenario D)" -ForegroundColor Yellow
    Run-Step -Name "mqtt-d-$qos" -Block {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $mqttD -MQTT_QOS $qos -MaxWaitSec $DMaxWait
    }
    Start-Sleep -Seconds $PauseSeconds
}

Switch-Broker "kafka"

Write-Host "`n--- KAFKA ---" -ForegroundColor Cyan
$kafkaAcksLevels = @("0","1","all")
foreach ($acks in $kafkaAcksLevels) {
    $devsForThisAcks = if ($Mode -eq "Full") { $DeviceCounts } else { if ($acks -eq "1") { $DeviceCounts } else { @(100) } }

    foreach ($devs in $devsForThisAcks) {
        $cs = "kafka_a_acks${acks}_dev${devs}_stats.csv"
        $cl = "kafka_a_acks${acks}_dev${devs}_lag.csv"
        Write-Host "`n>>> Kafka acks=$acks devices=$devs (~${ADuration}s) [proteklo: $([int]$swTotal.Elapsed.TotalMinutes) min]" -ForegroundColor Yellow
        Run-Step -Name "kafka-a-$acks-$devs" -Block {
            Reset-KafkaTopic
            $jS = Start-StatsJob $cs $devs
            $jL = Start-LagJob $cl ($ADuration + 3)
            Start-Sleep -Seconds $PauseSeconds
            & powershell -NoProfile -ExecutionPolicy Bypass -File $kafkaA -KAFKA_ACKS $acks -DeviceCount $devs -DurationSeconds $ADuration
            Start-Sleep -Seconds ($PauseSeconds + 1)
            Stop-JobSafe $jL; Stop-JobSafe $jS
            Assert-File $cs; Assert-File $cl
        }
        Start-Sleep -Seconds (Get-CooldownSeconds $devs)
    }

    $cs = "kafka_c_acks${acks}_dev100_stats.csv"
    $cl = "kafka_c_acks${acks}_dev100_lag.csv"
    Write-Host "`n>>> Kafka acks=$acks (Scenario C, burst)" -ForegroundColor Yellow
    Run-Step -Name "kafka-c-$acks" -Block {
        Reset-KafkaTopic
        $jS = Start-StatsJob $cs 100
        $jL = Start-LagJob $cl ($CNormalDur * 2 + $CBurstDur + 25)
        Start-Sleep -Seconds $PauseSeconds
        & powershell -NoProfile -ExecutionPolicy Bypass -File $kafkaC -KAFKA_ACKS $acks -DeviceCount 100 -InitialRateTotal 50 -BurstRateTotal 5000 -BurstDurationSeconds $CBurstDur -NormalDurationSeconds $CNormalDur
        Start-Sleep -Seconds ($PauseSeconds + 1)
        Stop-JobSafe $jL; Stop-JobSafe $jS
        Assert-File $cs; Assert-File $cl
    }
    Start-Sleep -Seconds $PauseSeconds

    Write-Host "`n>>> Kafka acks=$acks (Scenario D)" -ForegroundColor Yellow
    Run-Step -Name "kafka-d-$acks" -Block {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $kafkaD -KAFKA_ACKS $acks -MaxWaitSec $DMaxWait
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`n--- SCENARIO B (MQTT, ${DisconnectSec}s disconnect) ---" -ForegroundColor Cyan
Switch-BrokerForScenarioB "mqtt"
$leaf = "scenario_b_mqtt_stats.csv"
Run-Step -Name "scenario-b-mqtt" -Block {
    $jS = Start-StatsJob $leaf 0
    & powershell -NoProfile -ExecutionPolicy Bypass -File $scenB -TargetContainer iot-ingestion -TargetService ingestion -DisconnectSeconds $DisconnectSec -RecoverSeconds $RecoverSec -OutputFile "scenario_b_mqtt_results.csv"
    Stop-JobSafe $jS
    Assert-File $leaf
}
Start-Sleep -Seconds ($PauseSeconds + 1)

Write-Host "`n--- SCENARIO B (Kafka, ${DisconnectSec}s disconnect) ---" -ForegroundColor Cyan
Switch-BrokerForScenarioB "kafka"
$cs = "scenario_b_kafka_stats.csv"
$cl = "scenario_b_kafka_lag.csv"
Run-Step -Name "scenario-b-kafka" -Block {
    $jS = Start-StatsJob $cs 0
    $jL = Start-LagJob $cl ($DisconnectSec + $RecoverSec + 30)
    & powershell -NoProfile -ExecutionPolicy Bypass -File $scenB -TargetContainer iot-ingestion -TargetService ingestion -DisconnectSeconds $DisconnectSec -RecoverSeconds $RecoverSec -OutputFile "scenario_b_kafka_results.csv"
    Start-Sleep -Seconds ($PauseSeconds + 1)
    Stop-JobSafe $jL; Stop-JobSafe $jS
    Assert-File $cs; Assert-File $cl
}
Start-Sleep -Seconds $PauseSeconds

$env:STORAGE_ENABLED = "true"
$env:MQTT_TOPIC = "iot/sensors"
$env:KAFKA_TOPIC = "iot-sensors"
docker compose up -d --force-recreate storage analytics ingestion | Out-Null
$swTotal.Stop()
Write-Host "`n================================================================" -ForegroundColor Green
Write-Host "  ZAVRSENO za $([int]$swTotal.Elapsed.TotalMinutes) minuta. Sadrzaj results/:" -ForegroundColor Green
Get-ChildItem $resultsDir | ForEach-Object {
    $l = (Get-Content $_.FullName | Measure-Object -Line).Lines
    Write-Host ("    {0,-55} {1,6} linija  {2,8} B" -f $_.Name, $l, $_.Length)
}
Write-Host "================================================================" -ForegroundColor Green
