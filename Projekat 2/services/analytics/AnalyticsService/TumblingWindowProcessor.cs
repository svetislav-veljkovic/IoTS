using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class TumblingWindowProcessor : IAsyncDisposable
{
    private ConcurrentBag<SensorMessage> _currentWindow = new();
    private readonly int _windowSeconds;
    private readonly decimal _alertThreshold;
    private readonly ILogger<TumblingWindowProcessor> _logger;
    private readonly CancellationTokenSource _cts = new();
    private Task? _processingTask;

    public TumblingWindowProcessor(IConfiguration config, ILogger<TumblingWindowProcessor> logger)
    {
        _logger = logger;
        _windowSeconds = int.TryParse(config["TUMBLING_WINDOW_SECONDS"], out var ws) ? ws : 10;
        
        var thresholdStr = config["TEMPERATURE_ALERT_THRESHOLD"] ?? "50.0";
        _alertThreshold = decimal.TryParse(thresholdStr, NumberStyles.Any, CultureInfo.InvariantCulture, out var t) ? t : 50.0m;
    }

    public void Start()
    {
        _processingTask = ProcessWindowsAsync(_cts.Token);
        _logger.LogInformation("Tumbling Window pokrenut. Prozor: {Sec}s, Prag alarma: >{Threshold}°C", _windowSeconds, _alertThreshold);
    }

    public void AddMessage(SensorMessage message)
    {
        _currentWindow.Add(message);
    }

    private async Task ProcessWindowsAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_windowSeconds));
        try
        {
            while (await timer.WaitForNextTickAsync(token))
            {
                var messagesToProcess = Interlocked.Exchange(ref _currentWindow, new ConcurrentBag<SensorMessage>());
                
                if (messagesToProcess.IsEmpty) continue;

                var deviceStats = messagesToProcess
                    .GroupBy(m => m.Device_Id)
                    .Select(g => new 
                    {
                        DeviceId = g.Key,
                        AvgTemp = g.Average(m => m.Temperature),
                        MsgCount = g.Count(),
                      
                        AvgLatency = g.Where(m => m.LatencyCheck.HasValue)
                                      .Select(m => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - m.LatencyCheck!.Value)
                                      .DefaultIfEmpty(0)
                                      .Average()
                    })
                    .ToList();

                int alertCount = 0;
                foreach (var stat in deviceStats)
                {
                    if (stat.AvgTemp > _alertThreshold)
                    {
                        _logger.LogWarning("ALARM! Uredjaj {Device} | Temp: {Temp:F2}°C | Latency: {Latency:F0}ms | Uzoraka: {Count}", 
                            stat.DeviceId, stat.AvgTemp, stat.AvgLatency, stat.MsgCount);
                        alertCount++;
                    }
                }
                
                _logger.LogInformation("Prozor zatvoren: Obradjeno {TotalMsg} poruka. Detektovano {Alerts} alarma.", 
                    messagesToProcess.Count, alertCount);
            }
        }
        catch (OperationCanceledException) { }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_processingTask != null) await _processingTask;
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
    public long? LatencyCheck { get; set; } 
}