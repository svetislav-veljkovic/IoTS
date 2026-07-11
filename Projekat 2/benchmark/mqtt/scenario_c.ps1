param(
    [int]$MQTT_QOS = 1,
    [int]$DeviceCount = 100,
    [int]$InitialRateTotal = 50,
    [int]$BurstRateTotal = 5000,
    [int]$BurstDurationSeconds = 15,
    [int]$NormalDurationSeconds = 30
)
$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$Network    = "iot-network"
$BrokerHost = "iot-mosquitto"
$BrokerPort = 1883
$Topic      = "iot/sensors/device-%i"
$Msg        = '{"device_id":"device-%i","timestamp":"2025-01-01T00:00:00Z","ingested_at":0,"temperature":25.0,"humidity":60.0,"pressure":1013.25,"light":0,"sound":0,"motion":0,"battery":100.0,"location":"Zone-A"}'
$ConnRate   = [math]::Min(2000, [math]::Max(50, [int]($DeviceCount/3)))

$mqttDir    = $PSScriptRoot
$benchDir   = Split-Path $mqttDir -Parent
$resultsDir = Join-Path $benchDir "results"
if (!(Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }
$logFile = Join-Path $resultsDir "mqtt_c_qos${MQTT_QOS}_dev${DeviceCount}.log"
if (Test-Path $logFile) { Remove-Item $logFile -Force }

function Invoke-DockerSafe {
    param([string[]]$DockerArgs, [string]$Context)
    try {
        & docker @DockerArgs 2>&1
    } catch {
        Write-Host "[C] UPOZORENJE ($Context): docker komanda nije uspela: $($_.Exception.Message)" -ForegroundColor Yellow
        return $null
    }
}

function Get-Interval($totalTps, $devCount) {
    $perDeviceRate = [math]::Max($totalTps, 1) / [math]::Max($devCount, 1)

    return [math]::Max(1, [int][math]::Round(2000.0 / [math]::Max($perDeviceRate, 0.001)))
}

function Run-Phase($name, $tps, $dur) {
    $nameId = "mqtt_c_${name}_qos${MQTT_QOS}"
    Invoke-DockerSafe -DockerArgs @("rm","-f",$nameId) -Context "cleanup-pre-$name" | Out-Null
    $intv = Get-Interval $tps $DeviceCount
    Write-Host "[FAZA $name] tps=$tps interval=${intv}ms dur=${dur}s" -ForegroundColor Cyan
    $a = @("run","-d","--name",$nameId,"--network",$Network,"--ulimit","nofile=65536:65536",
           "emqx/emqtt-bench:latest","pub",
           "-h",$BrokerHost,"-p","$BrokerPort","-c","$DeviceCount","-R","$ConnRate",
           "-I","$intv","-L","0","-t",$Topic,"-m",$Msg,"-q","$MQTT_QOS","-w")

    $startResult = Invoke-DockerSafe -DockerArgs $a -Context "start-$name"
    if ($null -eq $startResult) {
        Write-Host "[FAZA $name] KRITICNO: docker run nije uspeo, preskacem ovu fazu." -ForegroundColor Red
    
        "GRESKA u fazi ${name}: docker run nije uspeo." | Out-File -FilePath $logFile -Append -Encoding utf8
        return
    }

    $dl = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $dl) {
        Start-Sleep -Seconds 2
        $l = Invoke-DockerSafe -DockerArgs @("logs",$nameId) -Context "wait-$name" | Out-String
        if ($l -match "start publishing") { break }
    }
    Start-Sleep -Seconds $dur
    Invoke-DockerSafe -DockerArgs @("kill","-s","SIGINT",$nameId) -Context "sigint-$name" | Out-Null
    Start-Sleep -Seconds 2
    Invoke-DockerSafe -DockerArgs @("kill",$nameId) -Context "kill-$name" | Out-Null

    $pl = Invoke-DockerSafe -DockerArgs @("logs",$nameId) -Context "final-$name" | Out-String
    if ([string]::IsNullOrWhiteSpace($pl)) {
    
        $pl = "GRESKA u fazi ${name}: docker logs nije vratio podatke (moguca Docker Desktop API greska)."
        Write-Host "[FAZA $name] UPOZORENJE: nisam uspeo da procitam logove." -ForegroundColor Yellow
    }
    $pl | Out-File -FilePath $logFile -Append -Encoding utf8
    Invoke-DockerSafe -DockerArgs @("rm","-f",$nameId) -Context "cleanup-post-$name" | Out-Null
}

Write-Host ""
Write-Host "=== MQTT Scenario C: Burst Load ===" -ForegroundColor Yellow
Write-Host "    QoS=$MQTT_QOS | Uredjaja=$DeviceCount"
Write-Host "    Normal=$InitialRateTotal | Burst=$BurstRateTotal"
Write-Host ""

Run-Phase "init"  $InitialRateTotal $NormalDurationSeconds
Run-Phase "burst" $BurstRateTotal   $BurstDurationSeconds
Run-Phase "recov" $InitialRateTotal $NormalDurationSeconds

Write-Host "[INFO] Log: $logFile" -ForegroundColor Green
