Write-Host "--- POKRECEM SVE TESTOVE ---" -ForegroundColor Green

function Run-K6-Test($scriptPath) {
    Write-Host "Pokrecem k6 skriptu: $scriptPath" -ForegroundColor Cyan
    & "$PSScriptRoot\k6.exe" run "$scriptPath"
}

$datasetPath = "$PSScriptRoot\..\..\data\data_set.csv"


Write-Host "[MQTT] Pokrecem prikupljanje statistike..." -ForegroundColor Gray
$mqttStats = Start-Process -FilePath "powershell.exe" -ArgumentList "-File .\collect_stats.ps1 mqtt_result.csv" -WindowStyle Hidden -PassThru

Write-Host "[MQTT] Pokrecem k6 scenarije hronoloski..." -ForegroundColor Yellow
Run-K6-Test "$PSScriptRoot\..\k6\scenario_a_mqtt.js"
Run-K6-Test "$PSScriptRoot\..\k6\scenario_c_burst_mqtt.js"
Run-K6-Test "$PSScriptRoot\..\k6\scenario_d_mqtt.js"

Stop-Process -Id $mqttStats.Id -Force
Write-Host "[MQTT] Statistika uspesno sacuvana u mqtt_result.csv" -ForegroundColor Green
Write-Host "--------------------------------------------------" -ForegroundColor Gray


Write-Host "[KAFKA] Pokrecem prikupljanje statistike..." -ForegroundColor Gray
$kafkaStats = Start-Process -FilePath "powershell.exe" -ArgumentList "-File .\collect_stats.ps1 kafka_result.csv" -WindowStyle Hidden -PassThru

Write-Host "[KAFKA] Pokrecem strimovanje stvarnog data_set.csv..." -ForegroundColor Yellow

Import-Csv $datasetPath | ForEach-Object {
    $_ | ConvertTo-Json -Compress
} | docker exec -i iot-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot-sensors

Write-Host "[KAFKA] Pokrecem k6 scenarije hronoloski..." -ForegroundColor Yellow
Run-K6-Test "$PSScriptRoot\..\k6\scenario_a_kafka.js"
Run-K6-Test "$PSScriptRoot\..\k6\scenario_c_burst_kafka.js"
Run-K6-Test "$PSScriptRoot\..\k6\scenario_d_kafka.js"

Stop-Process -Id $kafkaStats.Id -Force
Write-Host "[KAFKA] Statistika uspesno sacuvana u kafka_result.csv" -ForegroundColor Green
Write-Host "--------------------------------------------------" -ForegroundColor Gray


Write-Host "[SCENARIO B] Pokrecem prikupljanje statistike za mrežne prekide..." -ForegroundColor Gray
$scenarioBStats = Start-Process -FilePath "powershell.exe" -ArgumentList "-File .\collect_stats.ps1 scenario_b_result.csv" -WindowStyle Hidden -PassThru

Write-Host "[SCENARIO B] Pokrecem k6 saobracaj u pozadini..." -ForegroundColor Yellow
$scenarioBJob = Start-Process -FilePath "powershell.exe" -ArgumentList "-Command & '$PSScriptRoot\k6.exe' run '$PSScriptRoot\..\k6\scenario_a_mqtt.js'" -WindowStyle Hidden -PassThru


Start-Sleep -Seconds 3 

Write-Host "[SCENARIO B]  SIMULIRAM MREzNI PREKID NA 30 SEKUNDI " -ForegroundColor Red
docker network disconnect iot-network iot-mosquitto 2>$null

Start-Sleep -Seconds 30

Write-Host "[SCENARIO B] ZAVRSEN PREKID VRACAM MREZU NAZAD " -ForegroundColor Cyan
docker network connect iot-network iot-mosquitto 2>$null

Write-Host "[SCENARIO B] Cekam da pozadinski simulator zavrsi preostali striming..." -ForegroundColor Gray
Wait-Process -Id $scenarioBJob.Id

Stop-Process -Id $scenarioBStats.Id -Force
Write-Host "[SCENARIO B] Podaci o otpornosti sistema uspesno sacuvani u scenario_b_result.csv" -ForegroundColor Green
Write-Host "--------------------------------------------------" -ForegroundColor Gray

Write-Host "SVI TESTOVI SU USPESNO ZAVRSENI " -ForegroundColor Green