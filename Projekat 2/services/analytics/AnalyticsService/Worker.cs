using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly IConfiguration _cfg;
    private readonly TumblingWindowProcessor _proc;
    private readonly MqttAnalyticsConsumer _mqtt;
    private readonly KafkaAnalyticsConsumer _kafka;

    public Worker(ILogger<Worker> log, IConfiguration cfg,
                  TumblingWindowProcessor proc, MqttAnalyticsConsumer mqtt, KafkaAnalyticsConsumer kafka)
    { _log = log; _cfg = cfg; _proc = proc; _mqtt = mqtt; _kafka = kafka; }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var b = (_cfg["BROKER_TYPE"] ?? "mqtt").Trim().ToLowerInvariant();
        _log.LogInformation("Pokretanje Analytics, broker={B}", b);
        _proc.Start();
        try
        {
            if (b == "mqtt") { await _mqtt.StartAsync(stop); await Task.Delay(Timeout.Infinite, stop); }
            else if (b == "kafka") await _kafka.StartAsync(stop);
            else _log.LogCritical("Nepoznat BROKER_TYPE={B}", b);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }
}
