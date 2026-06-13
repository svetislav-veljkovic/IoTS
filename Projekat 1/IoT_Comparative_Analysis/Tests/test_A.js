import http from 'k6/http';
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';

const client = new grpc.Client();
client.load(['../GrpcApi/GrpcApi/Protos'], 'sensor.proto');


const csvData = new SharedArray('Real IoT Data', function () {
    const file = open('./iot_data.csv');
    return papaparse.parse(file, { header: true, skipEmptyLines: true }).data;
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
    const randomIndex = Math.floor(Math.random() * csvData.length);
    const record = csvData[randomIndex];

    if (!record) return;

   

    const deviceId = record.device_id || record.Device_ID || `dev_${Math.floor(Math.random() * 10)}`;
    
    // Parsiranje vremena
    const rawTime = record.timestamp || record.Timestamp;
    const isoTimestamp = rawTime ? new Date(rawTime).toISOString() : new Date().toISOString();
    const unixSeconds = Math.floor(new Date(isoTimestamp).getTime() / 1000);

    // Parsiranje brojeva (ako isNaN baci grešku, dodeljuje se ispravan broj)
    const temp = parseFloat(record.temperature || record.Temperature) || 25.0;
    const hum = parseFloat(record.humidity || record.Humidity) || 50.0;
    const co2 = parseInt(record.co2_level || record.CO2_Level || record.co2 || record.CO2) || 400;
    const volt = parseFloat(record.voltage || record.Voltage) || 12.0;
    const lat = parseFloat(record.gps_lat || record.GPS_Lat || record.Lat) || 43.0;
    const lng = parseFloat(record.gps_lng || record.GPS_Lon || record.Lng) || 21.0;

    const params = { headers: { 'Content-Type': 'application/json' } };


    const restPayload = JSON.stringify({
        deviceId: deviceId,
        timestamp: isoTimestamp,
        temperature: temp,
        humidity: hum,
        co2Level: co2,
        voltage: volt,
        gpsLat: lat,
        gpsLng: lng
    });
    
    const restRes = http.post('http://localhost:5001/api/SensorData/ingest', restPayload, params);
    check(restRes, { 'REST 200': (r) => r.status === 200 });


    client.connect('localhost:5002', { plaintext: true });
    
    const grpcRes = client.invoke('SensorService/IngestReading', {
        device_id: deviceId,
        timestamp: isoTimestamp, 
        temperature: temp,
        humidity: hum,
        co2_level: co2,
        voltage: volt,
        gps_lat: lat,
        gps_lng: lng
    });
    check(grpcRes, { 'gRPC OK': (r) => r && r.status === grpc.StatusOK });
    client.close();
 
    const gqlQuery = `
        mutation {
            ingestReading(
                deviceId: "${deviceId}",
                temperature: ${temp},
                humidity: ${hum},
                co2Level: ${co2},
                voltage: ${volt},
                gpsLat: ${lat},
                gpsLng: ${lng}
            )
        }
    `;
    
    const gqlRes = http.post('http://localhost:5003/graphql', JSON.stringify({ query: gqlQuery }), params);
    check(gqlRes, { 'GraphQL 200': (r) => r.status === 200 });

    sleep(0.1); 
};