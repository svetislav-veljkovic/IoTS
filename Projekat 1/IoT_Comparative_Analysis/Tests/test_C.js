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
    const params = { headers: { 'Content-Type': 'application/json' } };

    // REST 
    const restRes = http.get('http://localhost:5001/api/SensorData/aggregated/dev_A?days=30');
    check(restRes, { 'REST 200': (r) => r.status === 200 });

    // gRPC 
    client.connect('localhost:5002', { plaintext: true });
    
    
    const grpcRes = client.invoke('SensorService/GetAggregatedData', { 
        device_id: "dev_A", 
        days: 30 
    });
    check(grpcRes, { 'gRPC OK': (r) => r && r.status === grpc.StatusOK });
    client.close();

    // GraphQL 
  
    const gqlQuery = `
        query {
            getAggregatedData(deviceId: "dev_A", days: 30) {
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