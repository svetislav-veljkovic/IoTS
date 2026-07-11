# Kafka consumer lag merenje. Prima APSOLUTNI ResultsDir.
param(
    [Parameter(Mandatory=$true)][string]$ResultsDir,
    [Parameter(Mandatory=$true)][string]$OutputFile,
    [string]$GroupId = "iot-consumers",
    [int]$IntervalSeconds = 2,
    [int]$DurationSeconds = 60
)
$ErrorActionPreference = "Continue"
if (!(Test-Path $ResultsDir)) { New-Item -ItemType Directory -Path $ResultsDir | Out-Null }
$out = Join-Path $ResultsDir $OutputFile

"Timestamp,GroupId,Topic,Partition,CurrentOffset,LogEndOffset,Lag,ConsumerId,Host" | Out-File -FilePath $out -Encoding utf8
Write-Host "[lag] Kolektor pokrenut -> $out (grupa=$GroupId, ${DurationSeconds}s)"

$end = (Get-Date).AddSeconds($DurationSeconds)
$sample = 0
$rowsWritten = 0
try {
    while ((Get-Date) -lt $end) {
        $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $raw = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server localhost:9092 --describe --group $GroupId 2>&1
        $totalLag = 0
        $sampleRows = 0
        foreach ($line in $raw) {
            if ($line -notmatch "^$GroupId\s") { continue }
            $p = ($line -replace '\s+', ' ').Trim().Split(' ')
            if ($p.Length -lt 6) { continue }
            "$ts,$GroupId,$($p[1]),$($p[2]),$($p[3]),$($p[4]),$($p[5]),$($(if($p.Length -gt 6){$p[6]}else{''})),$($(if($p.Length -gt 7){$p[7]}else{''}))" |
                Out-File -FilePath $out -Append -Encoding utf8
            $sampleRows++
            $rowsWritten++
            if ($p[5] -match '^\d+$') { $totalLag += [long]$p[5] }
        }
        if ($sampleRows -eq 0) {
            $note = (($raw -join ' ') -replace ',', ';' -replace "`r|`n", ' ')
            "$ts,$GroupId,__no_rows__,-,-,-,-,,`"$note`"" | Out-File -FilePath $out -Append -Encoding utf8
            $rowsWritten++
        }
        $sample++
        if ($sample % 5 -eq 0) { Write-Host "[lag] $ts ukupni lag = $totalLag" }
        Start-Sleep -Seconds $IntervalSeconds
    }
}
finally {
    if ($rowsWritten -eq 0) {
        "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'),$GroupId,__collector_stopped_without_rows__,-,-,-,-,," |
            Out-File -FilePath $out -Append -Encoding utf8
    }
    Write-Host "[lag] Zaustavljen. Fajl: $out"
}
