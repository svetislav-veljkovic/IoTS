#!/bin/bash
echo "Pokrecem strimovanje stvarnog data_set.csv u Kafku..."


python3 -c "
import csv, json, time
with open('../data/data_set.csv', mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        row['Timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(json.dumps(row))
" | docker exec -i iot-kafka kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic iot-sensors

echo "Strimovanje završeno."