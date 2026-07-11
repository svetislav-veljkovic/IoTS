using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly IConfiguration _cfg;
    private readonly MqttConsumer _mqtt;
    private readonly KafkaConsumer _kafka;
    public Worker(ILogger<Worker> log, IConfiguration cfg, MqttConsumer mqtt, KafkaConsumer kafka)
    { _log = log; _cfg = cfg; _mqtt = mqtt; _kafka = kafka; }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var b = (_cfg["BROKER_TYPE"] ?? "mqtt").Trim().ToLowerInvariant();
        _log.LogInformation("Pokretanje Storage, broker={B}", b);
        try
        {
            if (b == "mqtt")
            {
                await _mqtt.StartAsync(stop);
                try { await Task.Delay(Timeout.Infinite, stop); }
                catch (OperationCanceledException) { await _mqtt.FlushRemaining(); }
            }
            else if (b == "kafka") await _kafka.StartAsync(stop);
            else _log.LogCritical("Nepoznat BROKER_TYPE={B}", b);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }
}
