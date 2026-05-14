import http from 'k6/http';
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';


const client = new grpc.Client();
client.load(['../GrpcApi/GrpcApi/Protos'], 'sensor.proto');

export const options = {
    stages: [
        { duration: '30s', target: 10 },  
        { duration: '1m', target: 100 },  
        { duration: '1m', target: 500 },  
        { duration: '30s', target: 0 },    
    ],
};

export default () => {
    const deviceId = `device_${Math.floor(Math.random() * 100)}`;
    
   
    const payload = JSON.stringify({
        deviceId: deviceId,
        timestamp: new Date().toISOString(),
        temperature: 24.5 + Math.random(),
        humidity: 55.0 + Math.random(),
        co2Level: 450,
        voltage: 12.6,
        gpsLat: 43.32,
        gpsLng: 21.89
    });

    const params = { headers: { 'Content-Type': 'application/json' } };

    
    const restRes = http.post('http://localhost:5001/api/SensorData/ingest', payload, params);
    check(restRes, { 'REST 200': (r) => r.status === 200 });

   

client.connect('localhost:5002', { plaintext: true });


const grpcPayload = {
    device_id: deviceId, 
   timestamp: new Date().toISOString(),
    temperature: 24.5 + Math.random(),
    humidity: 55.0 + Math.random(),
    co2_level: 450,
    voltage: 12.6,
    gps_lat: 43.32,
    gps_lng: 21.89
};

const grpcRes = client.invoke('SensorService/IngestReading', grpcPayload);

check(grpcRes, { 'gRPC OK': (r) => r && r.status === grpc.StatusOK });

  
    const gqlQuery = `
        mutation {
            ingestReading(
                deviceId: "${deviceId}",
                temperature: 24.5,
                humidity: 55.0,
                co2Level: 450,
                voltage: 12.6,
                gpsLat: 43.32,
                gpsLng: 21.89
            )
        }
    `;
    const gqlRes = http.post('http://localhost:5003/graphql', JSON.stringify({ query: gqlQuery }), params);
    check(gqlRes, { 'GraphQL 200': (r) => r.status === 200 });

    sleep(0.1); 
};