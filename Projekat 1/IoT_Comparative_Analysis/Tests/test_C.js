import http from 'k6/http';
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';

const client = new grpc.Client();

client.load(['../GrpcApi/GrpcApi/Protos'], 'sensor.proto');


const csvData = new SharedArray('Real IoT Data Devices for Test C', function () {
    const file = open('./iot_data.csv');
    return papaparse.parse(file, { header: true }).data;
});

export const options = {
    stages: [
        { duration: '30s', target: 10 },  
        { duration: '1m', target: 100 }, 
        { duration: '1m', target: 500 },  
        { duration: '30s', target: 0 },   
    ],
};

export default () => {
    const params = { headers: { 'Content-Type': 'application/json' } };

  
    const randomIndex = Math.floor(Math.random() * csvData.length);
    const realDeviceId = csvData[randomIndex].device_id || "device_1";


    const restRes = http.get(`http://localhost:5001/api/SensorData/aggregated/${realDeviceId}?days=30`);
    check(restRes, { 'REST 200': (r) => r.status === 200 });


    client.connect('localhost:5002', { plaintext: true });
    const grpcRes = client.invoke('SensorService/GetAggregatedData', { 
        device_id: realDeviceId, 
        days: 30 
    });
    check(grpcRes, { 'gRPC OK': (r) => r && r.status === grpc.StatusOK });
    client.close();

    const gqlQuery = `
        query {
            getAggregatedData(deviceId: "${realDeviceId}", days: 30) {
                avgTemperature
                maxCo2
                minVoltage
            }
        }
    `;
    const gqlRes = http.post('http://localhost:5003/graphql', JSON.stringify({ query: gqlQuery }), params);
    check(gqlRes, { 'GraphQL 200': (r) => r.status === 200 });

    sleep(0.1); 
};