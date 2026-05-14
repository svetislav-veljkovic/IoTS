
using Microsoft.AspNetCore.Mvc;
using Dapper;
using Npgsql;
using RestApi.Models;

namespace RestApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SensorDataController : ControllerBase
    {
        private readonly IConfiguration _config;
        private readonly string _connectionString;

        public SensorDataController(IConfiguration config)
        {
            _config = config;
            _connectionString = _config.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Connection string not found.");
        }

        
        [HttpPost("ingest")]
        public async Task<IActionResult> IngestReading([FromBody] SensorReadingDto dto)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            var sql = @"
                INSERT INTO sensor_readings 
                (device_id, timestamp, temperature, humidity, co2_level, voltage, gps_lat, gps_lng) 
                VALUES (@DeviceId, @Timestamp, @Temperature, @Humidity, @Co2Level, @Voltage, @GpsLat, @GpsLng)";

            await conn.ExecuteAsync(sql, dto);
            return Ok(new { message = "Data ingested successfully" });
        }

        
        [HttpGet("aggregated/{deviceId}")]
        public async Task<ActionResult<AggregatedDto>> GetAggregatedData(string deviceId, [FromQuery] int days = 30)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            var sql = @"
                SELECT 
                    AVG(temperature) as AvgTemperature, 
                    MAX(co2_level) as MaxCo2, 
                    MIN(voltage) as MinVoltage 
                FROM sensor_readings 
                WHERE device_id = @DeviceId 
                AND timestamp >= NOW() - INTERVAL '1 day' * @Days";

            var result = await conn.QueryFirstOrDefaultAsync<AggregatedDto>(sql, new { DeviceId = deviceId, Days = days });

            if (result == null) return NotFound();
            return Ok(result);
        }
    }
}
