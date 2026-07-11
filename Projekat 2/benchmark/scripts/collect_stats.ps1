# collect_stats.ps1 - PowerShell 5.1 / Docker Desktop tolerant collector
param(
    [Parameter(Mandatory=$true)][string]$ResultsDir,
    [Parameter(Mandatory=$true)][string]$OutputFile,
    [int]$TimeoutMs  = 25000,
    [int]$IntervalMs = 5000,
    [string[]]$Containers = @("iot-mosquitto","iot-kafka","iot-storage","iot-analytics","iot-ingestion","iot-postgres")
)
$ErrorActionPreference = "Continue"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

if (!(Test-Path $ResultsDir)) { New-Item -ItemType Directory -Path $ResultsDir | Out-Null }
$out = Join-Path $ResultsDir $OutputFile
"Timestamp,Container,CPUPerc,MemUsageMB,MemPerc,NetRxMB,NetTxMB,CallMs,Source,Note" | Out-File $out -Encoding utf8
"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),__collector_started__,,,,,,,collector,start" | Out-File $out -Append -Encoding utf8

function Parse-Bytes([string]$s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return 0.0 }
    $s = $s.Trim()
    if ($s -match '^([\d.]+)\s*(kB|KB|KiB)') { return [double]$matches[1] / 1024 }
    if ($s -match '^([\d.]+)\s*(MiB|MB)')    { return [double]$matches[1] }
    if ($s -match '^([\d.]+)\s*(GiB|GB)')    { return [double]$matches[1] * 1024 }
    if ($s -match '^([\d.]+)\s*B')           { return [double]$matches[1] / 1048576 }
    return 0.0
}

function Invoke-ProcessWithTimeout([string]$File, [string]$Arguments, [int]$WaitMs) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $File; $psi.Arguments = $Arguments
    $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false; $psi.CreateNoWindow = $true
    $p = New-Object System.Diagnostics.Process; $p.StartInfo = $psi
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        [void]$p.Start()
        $outTask = $p.StandardOutput.ReadToEndAsync()
        $errTask = $p.StandardError.ReadToEndAsync()
        if (-not $p.WaitForExit($WaitMs)) {
            try { $p.Kill() } catch {}
            return @{ Ok=$false; Out=""; Err="timeout >${WaitMs}ms"; Ms=$sw.ElapsedMilliseconds }
        }
        $stdout = $outTask.GetAwaiter().GetResult(); $stderr = $errTask.GetAwaiter().GetResult()
        return @{ Ok=($p.ExitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($stdout)); Out=$stdout; Err=$stderr; Ms=$sw.ElapsedMilliseconds }
    } catch {
        return @{ Ok=$false; Out=""; Err=$_.Exception.Message; Ms=$sw.ElapsedMilliseconds }
    } finally { $sw.Stop(); try { $p.Dispose() } catch {} }
}

function Write-StatsLines([string[]]$Lines, [long]$CallMs, [string]$Source) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    foreach ($line in $Lines) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $parts = $line -split '\|'
        if ($parts.Length -lt 5) { continue }
        $name = $parts[0].TrimStart('/'); $cpu = $parts[1] -replace '%',''
        $memUsed = 0.0; if ($parts[2] -match '^([\d.]+\s*\w+)\s*/') { $memUsed = Parse-Bytes $matches[1] }
        $memPerc = $parts[3] -replace '%',''; $rx=0.0; $tx=0.0
        if ($parts[4] -match '^(.+?)\s*/\s*(.+)$') { $rx=Parse-Bytes $matches[1]; $tx=Parse-Bytes $matches[2] }
        "$ts,$name,$cpu,$([math]::Round($memUsed,2)),$memPerc,$([math]::Round($rx,4)),$([math]::Round($tx,4)),$CallMs,$Source," | Out-File $out -Append -Encoding utf8
    }
}

$lastCpuUsec = @{}
$lastCpuAt   = @{}

function Invoke-CgroupSample([string]$Container, [int]$WaitMs) {
    $cpu = Invoke-ProcessWithTimeout "docker" "exec $Container cat /sys/fs/cgroup/cpu.stat" $WaitMs
    $mem = Invoke-ProcessWithTimeout "docker" "exec $Container cat /sys/fs/cgroup/memory.current" $WaitMs
    $max = Invoke-ProcessWithTimeout "docker" "exec $Container cat /sys/fs/cgroup/memory.max" $WaitMs
    $net = Invoke-ProcessWithTimeout "docker" "exec $Container cat /proc/net/dev" $WaitMs

    if (-not $cpu.Ok -or -not $mem.Ok -or -not $net.Ok) {
        return @{ Ok=$false; Out=""; Err="cgroup/proc read failed"; Ms=($cpu.Ms + $mem.Ms + $net.Ms) }
    }

    $cpuUsec = 0
    foreach ($line in ($cpu.Out -split "`r?`n")) {
        if ($line -match '^usage_usec\s+(\d+)') { $cpuUsec = [long]$matches[1]; break }
    }

    $rxBytes = 0L; $txBytes = 0L
    foreach ($line in ($net.Out -split "`r?`n")) {
        if ($line -match '^\s*(eth|en)[^:]*:\s*(.+)$') {
            $fields = ($matches[2].Trim() -split '\s+')
            if ($fields.Length -ge 16) {
                $rxBytes += [long]$fields[0]
                $txBytes += [long]$fields[8]
            }
        }
    }

    $memText = ($mem.Out.Trim() -split "`r?`n")[0]
    $maxText = if ($max.Ok) { ($max.Out.Trim() -split "`r?`n")[0] } else { "max" }
    return @{ Ok=$true; Out="$cpuUsec|$memText|$maxText|$rxBytes|$txBytes"; Err=""; Ms=($cpu.Ms + $mem.Ms + $max.Ms + $net.Ms) }
}

function Write-CgroupLine([string]$Container, [string]$Payload, [long]$CallMs) {
    $parts = $Payload.Trim() -split '\|'
    if ($parts.Length -lt 5) { return $false }

    $now = Get-Date
    $cpuUsec = 0.0; [void][double]::TryParse($parts[0], [ref]$cpuUsec)
    $memBytes = 0.0; [void][double]::TryParse($parts[1], [ref]$memBytes)
    $maxBytes = 0.0
    if ($parts[2] -ne "max") { [void][double]::TryParse($parts[2], [ref]$maxBytes) }
    $rxBytes = 0.0; [void][double]::TryParse($parts[3], [ref]$rxBytes)
    $txBytes = 0.0; [void][double]::TryParse($parts[4], [ref]$txBytes)

    $cpuPerc = ""
    if ($lastCpuUsec.ContainsKey($Container) -and $lastCpuAt.ContainsKey($Container)) {
        $elapsedUsec = [math]::Max(1.0, ($now - $lastCpuAt[$Container]).TotalMilliseconds * 1000.0)
        $deltaUsec = [math]::Max(0.0, $cpuUsec - [double]$lastCpuUsec[$Container])
        $cpuPerc = [math]::Round(($deltaUsec / $elapsedUsec) * 100.0, 2)
    }
    $lastCpuUsec[$Container] = $cpuUsec
    $lastCpuAt[$Container] = $now

    $memMb = [math]::Round($memBytes / 1048576.0, 2)
    $memPerc = ""
    if ($maxBytes -gt 0) { $memPerc = [math]::Round(($memBytes / $maxBytes) * 100.0, 2) }
    $rxMb = [math]::Round($rxBytes / 1048576.0, 4)
    $txMb = [math]::Round($txBytes / 1048576.0, 4)
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),$Container,$cpuPerc,$memMb,$memPerc,$rxMb,$txMb,$CallMs,cgroup-exec," | Out-File $out -Append -Encoding utf8
    return $true
}

$running = @(& docker ps --format "{{.Names}}" 2>$null)
$existing = @($Containers | Where-Object { $running -contains $_ })
if ($existing.Count -eq 0) { throw "Nijedan trazeni kontejner nije pokrenut." }
$fmt = '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.NetIO}}'
$failures = 0
$samplesWritten = 0

try {
    while ($true) {
        $args = "stats --no-stream --format `"$fmt`" " + ($existing -join ' ')
        $batch = Invoke-ProcessWithTimeout "docker" $args $TimeoutMs
        if ($batch.Ok) {
            Write-StatsLines ($batch.Out -split "`r?`n") $batch.Ms "docker-stats-batch"
            $samplesWritten++
            $failures = 0
        } else {
            $failures++
            $written = 0
            foreach ($c in $existing) {
                $cg = Invoke-CgroupSample $c ([math]::Max(1000, [int]($TimeoutMs / 3)))
                if ($cg.Ok -and (Write-CgroupLine $c $cg.Out $cg.Ms)) { $written++ }
            }
            if ($written -gt 0) {
                $samplesWritten++
                Write-Host "[stats] docker stats timeout; cgroup fallback uspeo za $written/$($existing.Count)." -ForegroundColor Yellow
            } else {
                $note = (($batch.Err + '; cgroup fallback failed') -replace ',', ';' -replace "`r|`n", ' ')
                "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),__collector_warning__,,,,,,,$($batch.Ms),stats-unavailable,`"$note`"" | Out-File $out -Append -Encoding utf8
                Write-Host "[stats] Nije uspeo ni docker stats ni cgroup uzorak ($note)." -ForegroundColor Yellow
            }
        }
        $backoff = [math]::Min(2, 1 + [math]::Floor($failures / 3))
        Start-Sleep -Milliseconds ($IntervalMs * $backoff)
    }
} finally {
    if ($samplesWritten -eq 0) {
        "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),__collector_warning__,,,,,,,collector,no_metric_samples" | Out-File $out -Append -Encoding utf8
    }
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),__collector_stopped__,,,,,,,collector,stop" | Out-File $out -Append -Encoding utf8
}
