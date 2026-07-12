using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace StorageService;

public class PostgresRepository
{
    private readonly string _conn;
    private readonly string _broker;
    private readonly ILogger<PostgresRepository> _log;

    public PostgresRepository(IConfiguration cfg, ILogger<PostgresRepository> log)
    {
        _conn = cfg["POSTGRES_CONNECTION_STRING"]
            ?? "Host=iot-postgres;Port=5432;Database=iot_data;Username=iotuser;Password=iotpassword";
        _broker = (cfg["BROKER_TYPE"] ?? "unknown").Trim().ToLowerInvariant();
        _log = log;
    }

    public async Task BulkInsertAsync(List<SensorMessage> msgs, CancellationToken ct = default)
    {
        if (msgs == null || msgs.Count == 0) return;
        const string sql = @"
            COPY sensor_readings
                (timestamp, device_id, temperature, humidity, pressure,
                 light, sound, motion, battery, location, broker_type, ingested_at)
            FROM STDIN (FORMAT BINARY)";
        try
        {
            await using var c = new NpgsqlConnection(_conn);
            await c.OpenAsync(ct);
            await using var w = await c.BeginBinaryImportAsync(sql, ct);
            foreach (var m in msgs)
            {
                await w.StartRowAsync(ct);
                await w.WriteAsync(m.Timestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, ct);
                await w.WriteAsync(m.DeviceId, NpgsqlDbType.Varchar, ct);
                await w.WriteAsync((double)m.Temperature, NpgsqlDbType.Double, ct);
                await w.WriteAsync((double)m.Humidity, NpgsqlDbType.Double, ct);
                await w.WriteAsync((double)m.Pressure, NpgsqlDbType.Double, ct);
                await w.WriteAsync(m.Light, NpgsqlDbType.Integer, ct);
                await w.WriteAsync(m.Sound, NpgsqlDbType.Integer, ct);
                await w.WriteAsync((short)m.Motion, NpgsqlDbType.Smallint, ct);
                await w.WriteAsync((double)m.Battery, NpgsqlDbType.Double, ct);
                await w.WriteAsync(m.Location, NpgsqlDbType.Varchar, ct);
                await w.WriteAsync(_broker, NpgsqlDbType.Varchar, ct);
                if (m.IngestedAt.HasValue) await w.WriteAsync(m.IngestedAt.Value, NpgsqlDbType.Double, ct);
                else await w.WriteNullAsync(ct);
            }
            await w.CompleteAsync(ct);
            _log.LogInformation("Upisano {N} poruka u sensor_readings ({B}).", msgs.Count, _broker);
        }
        catch (Exception ex) { _log.LogError(ex, "Greska pri bulk insert ({N} poruka).", msgs.Count); }
    }
}
