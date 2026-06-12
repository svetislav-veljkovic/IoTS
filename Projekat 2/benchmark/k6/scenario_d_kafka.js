import { Writer } from 'k6/x/kafka';
import encoding from 'k6/encoding';
import { SharedArray } from 'k6/data';

const csvData = new SharedArray('iot_dataset', function () {
    const fileContent = open('../../data/data_set.csv');
    const lines = fileContent.split(/\r?\n/);
    const header = lines[0].split(',');
    return [{ Device_Id: lines[1].split(',')[0], Temperature: lines[1].split(',')[2] }];  
});

const brokers = ['127.0.0.1:29092'];
const topic = 'iot-sensors';

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
    requiredAcks: 0,
    readTimeout: 60000000000,
    writeTimeout: 60000000000,
});

export const options = { scenarios: { default: { executor: 'per-vu-iterations', vus: 1, iterations: 1 } } };

export default function () {
    const payload = JSON.stringify({ 
        Timestamp: new Date().toISOString(),
        Device_Id: csvData[0].Device_Id,
        LatencyCheck: Date.now() 
    });

    const base64Payload = encoding.b64encode(payload);
    writer.produce({ messages: [{ value: base64Payload }] });
}

export function teardown() { writer.close(); }