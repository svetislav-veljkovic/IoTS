using Grpc.Core;
using Dapper;
using Npgsql;
using Google.Protobuf.WellKnownTypes;

namespace GrpcApi.Services
{
    
    public class SensorGrpcService : SensorService.SensorServiceBase
    {
        private readonly string _connectionString;

        public SensorGrpcService(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Connection string is missing.");
        }

       
        public override async Task<IngestResponse> IngestReading(ReadingRequest request, ServerCallContext context)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            var sql = @"
                INSERT INTO sensor_readings 
                (device_id, timestamp, temperature, humidity, co2_level, voltage, gps_lat, gps_lng) 
                VALUES (@DeviceId, @Timestamp, @Temperature, @Humidity, @Co2Level, @Voltage, @GpsLat, @GpsLng)";

            await conn.ExecuteAsync(sql, new
            {
                DeviceId = request.DeviceId,
                Timestamp = request.Timestamp.ToDateTime(), // Pretvaramo Protobuf Timestamp u C# DateTime
                Temperature = request.Temperature,
                Humidity = request.Humidity,
                Co2Level = request.Co2Level,
                Voltage = request.Voltage,
                GpsLat = request.GpsLat,
                GpsLng = request.GpsLng
            });

            return new IngestResponse { Success = true, Message = "Data ingested via gRPC binary protocol" };
        }

       
        public override async Task<AggregatedResponse> GetAggregatedData(AggregatedRequest request, ServerCallContext context)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            var sql = @"
                SELECT 
                    AVG(temperature) as avgtemperature, 
                    MAX(co2_level) as maxco2, 
                    MIN(voltage) as minvoltage 
                FROM sensor_readings 
                WHERE device_id = @DeviceId 
                AND timestamp >= NOW() - INTERVAL '1 day' * @Days";

            
            var res = await conn.QueryFirstOrDefaultAsync(sql, new { DeviceId = request.DeviceId, Days = request.Days });

            return new AggregatedResponse
            {
                AvgTemperature = (double)(res?.avgtemperature ?? 0.0),
                MaxCo2 = (int)(res?.maxco2 ?? 0),
                MinVoltage = (double)(res?.minvoltage ?? 0.0)
            };
        }
    }
}