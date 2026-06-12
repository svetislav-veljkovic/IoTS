import ws from 'k6/ws';
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

export const options = { stages: [{ duration: '5s', target: 500 }, { duration: '10s', target: 500 }] };

export default function () {
    const url = 'ws://localhost:9001';
    
    const rowIndex = (__VU + __ITER) % csvData.length;
    const row = csvData[rowIndex];

    ws.connect(url, {}, (socket) => {
        socket.send(JSON.stringify({ 
            Device_Id: row.Device_Id || 'burst-dev', 
            Temperature: row.Temperature 
        }));
        socket.close();
    });
}