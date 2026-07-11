param(
    [int]$MQTT_QOS = 1,
    [int]$DeviceCount = 100,
    [int]$MsgRatePerDevice = 10,
    [int]$DurationSeconds = 60
)
$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$Network    = "iot-network"
$BrokerHost = "iot-mosquitto"
$BrokerPort = 1883
$Topic      = "iot/sensors/device-%i"
$Msg        = '{"device_id":"device-%i","timestamp":"2025-01-01T00:00:00Z","ingested_at":0,"temperature":25.0,"humidity":60.0,"pressure":1013.25,"light":0,"sound":0,"motion":0,"battery":100.0,"location":"Zone-A"}'

$TotalTps = [int64]$DeviceCount * [int64]$MsgRatePerDevice


$IntervalMs = [math]::Max(1, [int][math]::Round(2000.0 / [math]::Max($MsgRatePerDevice, 1)))


if ($DeviceCount -ge 5000) {
    $ConnRate = 100
} elseif ($DeviceCount -ge 1000) {
    $ConnRate = [math]::Min(500, [math]::Max(50, [int]($DeviceCount / 4)))
} else {
    $ConnRate = [math]::Min(2000, [math]::Max(50, [int]($DeviceCount / 3)))
}

$BenchName   = "mqttbench_a_qos${MQTT_QOS}_dev${DeviceCount}"

$mqttDir    = $PSScriptRoot
$benchDir   = Split-Path $mqttDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "mqtt_a_qos${MQTT_QOS}_dev${DeviceCount}.log"
if (Test-Path $logFile) { Remove-Item $logFile -Force }

Write-Host ""
Write-Host "=== MQTT Scenario A: Massive Sensor Ingestion ===" -ForegroundColor Yellow
Write-Host "    QoS           = $MQTT_QOS"
Write-Host "    Paralelnih    = $DeviceCount (-c)"
Write-Host "    Rate/device   = $MsgRatePerDevice msg/s"
Write-Host "    Ukupan target = ~$TotalTps msg/s"
Write-Host "    Interval (-I) = $IntervalMs ms (po uredjaju, nezavisno od broja uredjaja)"
Write-Host "    Conn rate     = $ConnRate conn/s (-R)"
Write-Host "    Trajanje      = $DurationSeconds s"
Write-Host ""

function Invoke-DockerSafe {
    param([string[]]$DockerArgs, [string]$Context, [int]$TimeoutSec = 45)
    $job = $null
    try {
        $job = Start-Job -ScriptBlock { param($da) & docker @da 2>&1 } -ArgumentList (,$DockerArgs)
        if (Wait-Job -Job $job -Timeout $TimeoutSec) {
            $result = Receive-Job -Job $job -ErrorAction SilentlyContinue
            return $result
        } else {
            Write-Host "[bench] UPOZORENJE ($Context): docker komanda nije odgovorila u ${TimeoutSec}s. Prekidam i nastavljam." -ForegroundColor Yellow
            try { Stop-Job -Job $job -ErrorAction SilentlyContinue } catch {}
            return $null
        }
    } catch {
        Write-Host "[bench] UPOZORENJE ($Context): docker komanda nije uspela: $($_.Exception.Message)" -ForegroundColor Yellow
        return $null
    } finally {
        if ($job) { try { Remove-Job -Job $job -Force -ErrorAction SilentlyContinue } catch {} }
    }
}

function Invoke-DockerSafeRetry {
    param([string[]]$DockerArgs, [string]$Context, [int]$TimeoutSec = 45, [int]$Retries = 3, [int]$DelaySec = 5)
    for ($i = 1; $i -le $Retries; $i++) {
        $r = Invoke-DockerSafe -DockerArgs $DockerArgs -Context "$Context (pokusaj $i/$Retries)" -TimeoutSec $TimeoutSec
        if ($null -ne $r) { return $r }
        if ($i -lt $Retries) {
            Write-Host "[bench] Ponovni pokusaj za '$Context' za ${DelaySec}s..." -ForegroundColor DarkYellow
            Start-Sleep -Seconds $DelaySec
        }
    }
    return $null
}

$dynTimeout = if ($DeviceCount -ge 5000) { 90 } elseif ($DeviceCount -ge 1000) { 60 } else { 45 }

Invoke-DockerSafe -DockerArgs @("rm","-f",$BenchName) -Context "cleanup-pre" -TimeoutSec 20 | Out-Null

$args_run = @(
    "run","-d","--name",$BenchName,
    "--network",$Network,
    "--ulimit","nofile=65536:65536",
    "emqx/emqtt-bench:latest","pub",
    "-h",$BrokerHost,"-p","$BrokerPort",
    "-c","$DeviceCount",
    "-R","$ConnRate",
    "-I","$IntervalMs",
    "-L","0",
    "-t",$Topic,"-m",$Msg,"-q","$MQTT_QOS","-w"
)

$startResult = Invoke-DockerSafeRetry -DockerArgs $args_run -Context "start-bench" -TimeoutSec $dynTimeout -Retries 2 -DelaySec 8
if ($null -eq $startResult) {
    Write-Host "[bench] KRITICNO: Nije uspelo pokretanje bench kontejnera (docker run timeout/greska)." -ForegroundColor Red
    Write-Host "         Ovo obicno znaci da je Docker Desktop engine preopterecen trenutnim brojem konekcija." -ForegroundColor Red
    Write-Host "         Preskacem ovaj run. Log: $logFile" -ForegroundColor Red
    "GRESKA: docker run nije uspeo (timeout ili Docker Desktop preopterecenje/500 error)." | Out-File -FilePath $logFile -Encoding utf8
    exit 1
}

Write-Host "[bench] Kontejner $BenchName pokrenut. Cekam da se svi klijenti povezu..." -ForegroundColor DarkGray

$waitCap = [math]::Max(30, [int]($DeviceCount / 150) + 10)
if ($DeviceCount -ge 5000) { $waitCap = [math]::Max($waitCap, 150) }
elseif ($DeviceCount -ge 1000) { $waitCap = [math]::Max($waitCap, 45) }

$pollEvery = if ($DeviceCount -ge 1000) { 8 } else { 2 }
$deadline = (Get-Date).AddSeconds($waitCap)
$elapsedLoop = 0
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds $pollEvery
    $elapsedLoop += $pollEvery
    $l = Invoke-DockerSafe -DockerArgs @("logs",$BenchName) -Context "wait-connect" -TimeoutSec 20 | Out-String
    if ($l -match "start publishing") { break }
    Write-Host "[bench] ... i dalje cekam konekcije (~${elapsedLoop}s / ${waitCap}s)" -ForegroundColor DarkGray
}

Write-Host "[bench] MERIM $DurationSeconds sekundi..." -ForegroundColor Cyan
Start-Sleep -Seconds $DurationSeconds

Write-Host "[bench] Zaustavljam (SIGINT za summary)..." -ForegroundColor DarkGray
Invoke-DockerSafe -DockerArgs @("kill","-s","SIGINT",$BenchName) -Context "sigint" -TimeoutSec 20 | Out-Null
$flushWait = if ($DeviceCount -ge 5000) { 8 } elseif ($DeviceCount -ge 1000) { 5 } else { 3 }
Start-Sleep -Seconds $flushWait
Invoke-DockerSafe -DockerArgs @("kill",$BenchName) -Context "kill" -TimeoutSec 20 | Out-Null

$all = Invoke-DockerSafeRetry -DockerArgs @("logs",$BenchName) -Context "final-logs" -TimeoutSec 30 -Retries 3 -DelaySec 5 | Out-String
if ([string]::IsNullOrWhiteSpace($all)) {
    $all = "GRESKA: docker logs nije vratio podatke ni posle 3 pokusaja (Docker Desktop API greska/timeout tokom testa). " +
           "Proveri rucno: docker logs $BenchName"
    Write-Host "[bench] UPOZORENJE: nisam uspeo da procitam logove kontejnera ni posle retry-ja. Vidi napomenu u log fajlu." -ForegroundColor Yellow
}
$all | Out-File -FilePath $logFile -Encoding utf8
$all -split "`n" | Where-Object {
    $_ -match '^\d+[ms]\s+(pub|connect_succ|connect_fail|pub_succ|pub_overrun)' -or
    $_ -match '^publish (complete|finish)'
} | ForEach-Object { Write-Host $_ }

Invoke-DockerSafe -DockerArgs @("rm","-f",$BenchName) -Context "cleanup-post" -TimeoutSec 20 | Out-Null
Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
