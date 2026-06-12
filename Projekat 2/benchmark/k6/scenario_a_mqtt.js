import ws from 'k6/ws';
import { check, sleep } from 'k6';
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

export const options = {
    vus: 100,
    duration: '30s',
};

export default function () {
    const url = 'ws://localhost:9001'; 
    const params = { tags: { my_tag: 'mqtt_sensor' } };

    const rowIndex = (__VU + __ITER) % csvData.length;
    const row = csvData[rowIndex];

    const res = ws.connect(url, params, function (socket) {
        socket.on('open', () => {
            const payload = JSON.stringify({
                Timestamp: new Date().toISOString(),
                Device_Id: row.Device_Id || `k6-sensor-${__VU}`,
                Temperature: row.Temperature,
                Humidity: row.Humidity,
                Pressure: row.Pressure,
                Light: row.Light,
                Sound: row.Sound,
                Motion: row.Motion,
                Battery: row.Battery,
                Location: row.Location || "Zone-A"
            });

            socket.send(payload);
        });

        socket.on('error', (e) => {
            if (e.error() != 'websocket: close sent') {
                console.log('An unexpected error occurred: ', e.error());
            }
        });

        sleep(1);
        socket.close();
    });

    check(res, { 'status is 101': (r) => r && r.status === 101 });
}