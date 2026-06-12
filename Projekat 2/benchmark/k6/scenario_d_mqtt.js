import ws from 'k6/ws';
import { SharedArray } from 'k6/data';

const csvData = new SharedArray('iot_dataset', function () {
    const fileContent = open('../../data/data_set.csv');
    const lines = fileContent.split(/\r?\n/);
    return [{ Device_Id: lines[1].split(',')[0], Temperature: lines[1].split(',')[2] }];
});

export default function () {
    const url = 'ws://localhost:9001';
    ws.connect(url, {}, (socket) => {
        const payload = JSON.stringify({
            Timestamp: new Date().toISOString(),
            Device_Id: csvData[0].Device_Id,
            Temperature: csvData[0].Temperature,
            LatencyCheck: Date.now() 
        });
        socket.send(payload);
        socket.close();
    });
}