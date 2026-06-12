using System.Data;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class PostgresRepository
{
    private readonly string _connectionString;
    private readonly ILogger<PostgresRepository> _logger;

    public PostgresRepository(IConfiguration configuration, ILogger<PostgresRepository> logger)
    {
      
        _connectionString = configuration["POSTGRES_CONNECTION_STRING"]
            ?? "Host=postgres;Port=5432;Database=iot_data;Username=iotuser;Password=iotpassword;";
        _logger = logger;
    }

    public async Task BulkInsertAsync(List<SensorMessage> messages)
    {
        if (messages == null || messages.Count == 0) return;

        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

           
            await using var writer = await conn.BeginBinaryImportAsync(
                "COPY sensor_data (timestamp, device_id, temperature, humidity, pressure, light, sound, motion, battery, location) FROM STDIN (FORMAT BINARY)"
            );

            foreach (var msg in messages)
            {
                await writer.StartRowAsync();
                await writer.WriteAsync(msg.Timestamp.ToUniversalTime(), NpgsqlTypes.NpgsqlDbType.TimestampTz);
                await writer.WriteAsync(msg.Device_Id, NpgsqlTypes.NpgsqlDbType.Varchar);
                await writer.WriteAsync(msg.Temperature, NpgsqlTypes.NpgsqlDbType.Numeric);
                await writer.WriteAsync(msg.Humidity, NpgsqlTypes.NpgsqlDbType.Numeric);
                await writer.WriteAsync(msg.Pressure, NpgsqlTypes.NpgsqlDbType.Numeric);
                await writer.WriteAsync(msg.Light, NpgsqlTypes.NpgsqlDbType.Integer);
                await writer.WriteAsync(msg.Sound, NpgsqlTypes.NpgsqlDbType.Integer);
                await writer.WriteAsync(msg.Motion, NpgsqlTypes.NpgsqlDbType.Integer);
                await writer.WriteAsync(msg.Battery, NpgsqlTypes.NpgsqlDbType.Numeric);
                await writer.WriteAsync(msg.Location, NpgsqlTypes.NpgsqlDbType.Varchar);
            }

            await writer.CompleteAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Greska prilikom bulk insert-a u Postgres.");
        }
    }
}


public class SensorMessage
{
    public DateTime Timestamp { get; set; }
    public string Device_Id { get; set; } = string.Empty;
    public decimal Temperature { get; set; }
    public decimal Humidity { get; set; }
    public decimal Pressure { get; set; }
    public int Light { get; set; }
    public int Sound { get; set; }
    public int Motion { get; set; }
    public decimal Battery { get; set; }
    public string Location { get; set; } = string.Empty;
}