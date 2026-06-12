# 1. Osiguraj da folder postoji
$resultsDir = "..\results"
if (!(Test-Path $resultsDir)) {
    New-Item -ItemType Directory -Path $resultsDir | Out-Null
}


$izlazniNaziv = $args[0]
if ($null -eq $izlazniNaziv) {
    
    $izlazniNaziv = "docker_stats_$(Get-Date -UFormat %s).csv"
}


$outputFile = Join-Path $resultsDir $izlazniNaziv

"Timestamp,Container,CPU %,Mem Usage (Trenutno / Limit),Mem %,Net I/O,Block I/O" | Out-File -FilePath $outputFile -Encoding utf8
Write-Host "Prikupljanje podataka pocelo. Rezultati u: $outputFile" -ForegroundColor Cyan

while ($true) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    
    $containers = docker ps --format "{{.Names}}" | Select-String "iot|projekat2"
    
    foreach ($container in $containers) {
        $containerName = $container.ToString().Trim()
        
        
        $stats = & docker stats $containerName --no-stream --format "$timestamp,{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}" 2>$null
        
        if ($LASTEXITCODE -eq 0) {
            $stats | Out-File -FilePath $outputFile -Append -Encoding utf8
        }
    }
    
    Start-Sleep -Seconds 1
}