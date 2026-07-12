using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace AnalyticsService;

public class TumblingWindowProcessor : IAsyncDisposable
{
    private ConcurrentBag<SensorMessage> _window = new();
    private readonly int _windowSeconds;
    private readonly decimal _threshold;
    private readonly ILogger<TumblingWindowProcessor> _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly string _conn;
    private readonly string _broker;
    private readonly MaasClient _maas;
    private Task? _task;

    public TumblingWindowProcessor(IConfiguration config, ILogger<TumblingWindowProcessor> logger, MaasClient maas)
    {
        _logger = logger;
        _maas = maas;
        _windowSeconds = int.TryParse(config["TUMBLING_WINDOW_SECONDS"], out var ws) ? ws : 10;
        _threshold = decimal.TryParse(config["TEMPERATURE_ALERT_THRESHOLD"],
            System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var t) ? t : 50.0m;
        _conn = config["POSTGRES_CONNECTION_STRING"]
            ?? "Host=iot-postgres;Port=5432;Database=iot_data;Username=iotuser;Password=iotpassword";
        _broker = (config["BROKER_TYPE"] ?? "unknown").Trim().ToLowerInvariant();
    }

    public void Start()
    {
        _task = ProcessAsync(_cts.Token);
        _logger.LogInformation("TumblingWindow pokrenut: prozor={Sec}s, prag={Th}C", _windowSeconds, _threshold);
    }

    public void AddMessage(SensorMessage m)
    {
        _window.Add(m);
    }

    private async Task ProcessAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_windowSeconds));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var batch = Interlocked.Exchange(ref _window, new ConcurrentBag<SensorMessage>());
                if (batch.IsEmpty) continue;

                var winEnd = DateTimeOffset.UtcNow;
                var winStart = winEnd.AddSeconds(-_windowSeconds);
                var nowMs = winEnd.ToUnixTimeMilliseconds();

                var allTemps = batch.Select(x => (double)x.Temperature).ToList();
                var avgTemp = allTemps.Average();
                var maxTemp = allTemps.Max();
                var minTemp = allTemps.Min();
                var avgHum = batch.Average(x => (double)x.Humidity);
                var msgCount = batch.Count;
                var alert = avgTemp > (double)_threshold;
                var maasPrediction = await _maas.PredictAsync(new MaasPredictionRequest
                {
                    MessageCount = msgCount,
                    AvgTemperature = avgTemp,
                    MaxTemperature = maxTemp,
                    MinTemperature = minTemp,
                    AvgHumidity = avgHum,
                    AvgBattery = batch.Average(x => (double)x.Battery),
                    MotionRatio = batch.Average(x => (double)x.Motion),
                    WindowSeconds = _windowSeconds
                }, ct);

                var timedMessages = batch.Where(x => x.IngestedAt.HasValue).ToList();
                var avgE2eMs = timedMessages
                    .Select(x => (double)(nowMs - (long)(x.IngestedAt!.Value * 1000d)))
                    .DefaultIfEmpty(0d)
                    .Average();

                var scenarioDMessages = batch
                    .Where(x => string.Equals(x.Location, "CRITICAL-ZONE", StringComparison.OrdinalIgnoreCase))
                    .ToList();
                var marker = scenarioDMessages.Count > 0 ? "CRITICAL-ZONE" : "-";
                var markerE2eMs = scenarioDMessages
                    .Where(x => x.IngestedAt.HasValue)
                    .Select(x => (double)(nowMs - (long)(x.IngestedAt!.Value * 1000d)))
                    .DefaultIfEmpty(avgE2eMs)
                    .Average();

                if (alert)
                    _logger.LogWarning(
                        "CRITICAL ALERT (window avg)! AvgTemp={Avg:F2}C Max={Max:F2}C E2ELatency={Lat:F0}ms N={N} Broker={B} Lokacija={Loc}",
                        avgTemp, maxTemp, markerE2eMs, msgCount, _broker, marker);

                if (maasPrediction != null && maasPrediction.RiskLevel != "normal")
                    _logger.LogWarning(
                        "MaaS ML alert! risk={Risk} score={Score:F3} model={Model} avgTemp={Avg:F2} maxTemp={Max:F2} N={N}",
                        maasPrediction.RiskLevel, maasPrediction.RiskScore, maasPrediction.ModelName, avgTemp, maxTemp, msgCount);

                _logger.LogInformation(
                    "Prozor zatvoren [{Start:HH:mm:ss}-{End:HH:mm:ss}]: poruke={Tot} avg={Avg:F2}C e2e={E2E:F0}ms alert={Al} ml={Risk} broker={B}",
                    winStart.LocalDateTime, winEnd.LocalDateTime, msgCount, avgTemp, avgE2eMs, alert, maasPrediction?.RiskLevel ?? "n/a", _broker);

                _ = Task.Run(() => SaveWindowAsync(winStart, winEnd, msgCount, avgTemp, maxTemp, minTemp, avgHum, alert, avgE2eMs, maasPrediction), CancellationToken.None);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task SaveWindowAsync(DateTimeOffset winStart, DateTimeOffset winEnd,
        int msgCount, double avgTemp, double maxTemp, double minTemp, double avgHum,
        bool alertTriggered, double avgE2eMs, MaasPredictionResponse? maasPrediction)
    {
        const string sql = @"
            INSERT INTO analytics_windows
                (window_start, window_end, broker_type, message_count,
                 avg_temperature, max_temperature, min_temperature, avg_humidity,
                 alert_triggered, avg_e2e_ms, maas_risk_level, maas_risk_score, maas_model_name)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)";
        try
        {
            await using var c = new NpgsqlConnection(_conn);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, c);
            cmd.Parameters.AddWithValue(winStart.UtcDateTime);
            cmd.Parameters.AddWithValue(winEnd.UtcDateTime);
            cmd.Parameters.AddWithValue(_broker);
            cmd.Parameters.AddWithValue(msgCount);
            cmd.Parameters.AddWithValue(avgTemp);
            cmd.Parameters.AddWithValue(maxTemp);
            cmd.Parameters.AddWithValue(minTemp);
            cmd.Parameters.AddWithValue(avgHum);
            cmd.Parameters.AddWithValue(alertTriggered);
            cmd.Parameters.AddWithValue(avgE2eMs);
            cmd.Parameters.AddWithValue((object?)maasPrediction?.RiskLevel ?? DBNull.Value);
            cmd.Parameters.AddWithValue((object?)maasPrediction?.RiskScore ?? DBNull.Value);
            cmd.Parameters.AddWithValue((object?)maasPrediction?.ModelName ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Greska pri upisu analytics_windows.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_task != null) await _task;
        _cts.Dispose();
    }
}
