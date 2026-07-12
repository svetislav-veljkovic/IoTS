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
    private readonly CepEventConsumer _cep;

    public Worker(ILogger<Worker> log, IConfiguration cfg,
                  TumblingWindowProcessor proc, MqttAnalyticsConsumer mqtt, CepEventConsumer cep)
    { _log = log; _cfg = cfg; _proc = proc; _mqtt = mqtt; _cep = cep; }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var b = (_cfg["BROKER_TYPE"] ?? "mqtt").Trim().ToLowerInvariant();
        _log.LogInformation("Pokretanje Analytics, broker={B}", b);
        _proc.Start();
        try
        {
            if (b == "mqtt")
            {
                await _mqtt.StartAsync(stop);
                await _cep.StartAsync(stop);
                await Task.Delay(Timeout.Infinite, stop);
            }
            else _log.LogCritical("Nepoznat BROKER_TYPE={B}", b);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }
}
