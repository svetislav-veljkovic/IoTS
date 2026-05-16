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
    const deviceId = "dev_A";
    const isoTimestamp = new Date().toISOString(); 
    const params = { headers: { 'Content-Type': 'application/json' } };

    //  REST 
    const restPayload = JSON.stringify({
        deviceId: deviceId,
        timestamp: isoTimestamp,
        temperature: 25.0,
        humidity: 50.0,
        co2Level: 400,
        voltage: 12.0,
        gpsLat: 43.0,
        gpsLng: 21.0
    });
    
    const restRes = http.post('http://localhost:5001/api/SensorData/ingest', restPayload, params);
    check(restRes, { 'REST 200': (r) => r.status === 200 });

    // gRPC 
    client.connect('localhost:5002', { plaintext: true });
    
    const grpcRes = client.invoke('SensorService/IngestReading', {
        device_id: deviceId,
        timestamp: isoTimestamp, 
        temperature: 25.0,
        humidity: 50.0,
        co2_level: 400,
        voltage: 12.0,
        gps_lat: 43.0,
        gps_lng: 21.0
    });
    check(grpcRes, { 'gRPC OK': (r) => r && r.status === grpc.StatusOK });
    client.close();

    // GraphQL 
    const gqlQuery = `
        mutation {
            ingestReading(
                deviceId: "${deviceId}",
                temperature: 25.0,
                humidity: 50.0,
                co2Level: 400,
                voltage: 12.0,
                gpsLat: 43.0,
                gpsLng: 21.0
            )
        }
    `;
    
    const gqlRes = http.post('http://localhost:5003/graphql', JSON.stringify({ query: gqlQuery }), params);
    check(gqlRes, { 'GraphQL 200': (r) => r.status === 200 });

    sleep(0.1); 
};