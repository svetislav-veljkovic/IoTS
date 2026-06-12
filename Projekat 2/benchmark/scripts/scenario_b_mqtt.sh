#!/bin/bash
echo "--- SCENARIO B: MQTT Network Failure ---"
docker network disconnect iot-network iot-storage
echo "Mreza prekinuta. Cekam 30s..."
sleep 30
docker network connect iot-network iot-storage
echo "Mreza vracena. Proverite da li je MQTT client automatski nastavio rad."