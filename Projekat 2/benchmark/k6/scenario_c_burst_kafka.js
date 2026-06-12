import { check } from 'k6';
import { Writer } from 'k6/x/kafka';
import encoding from 'k6/encoding';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';

const csvData = new SharedArray('iot_dataset', function () {
   const fileContent = open('../../data/data_set.csv');
    const lines = fileContent.split(/\r?\n/);
    const header = lines[0].split(',');
    
    let result = [];
    for (let i = 1; i < lines.length; i++) {
        if (!lines[i].trim()) continue;
        let currentline = lines[i].split(',');
        let obj = {};
        for (let j = 0; j < header.length; j++) {
            obj[header[j].trim()] = currentline[j] ? currentline[j].trim() : '';
        }
        result.push(obj);
    }
    return result;
});

const brokers = ['127.0.0.1:29092'];
const topic = 'iot-sensors';

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
    batchSize: 50,
    batchTimeout: 50000000,       
    writeTimeout: 60000000000,    
    readTimeout: 60000000000,     
    requiredAcks: 0,              
});

export const options = {
    stages: [
        { duration: '5s', target: 1000 },  
        { duration: '10s', target: 1000 }, 
        { duration: '5s', target: 0 },     
    ],
};

export default function () {
    const rowIndex = (__VU + __ITER) % csvData.length;
    const row = csvData[rowIndex];

    const payload = JSON.stringify({
        Timestamp: new Date().toISOString(),
        Device_Id: row.Device_Id || `burst-kafka-${__VU}`,
        Temperature: row.Temperature
    });

    const base64Payload = encoding.b64encode(payload);

    const error = writer.produce({ 
        messages: [{ value: base64Payload }] 
    });
    
    check(error, { 'poslato': (e) => !e });

    sleep(0.05); 
}

export function teardown() {
    writer.close();
}