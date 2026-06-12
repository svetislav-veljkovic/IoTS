using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly MqttConsumer _mqttConsumer;
    private readonly KafkaConsumer _kafkaConsumer;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration config,
        MqttConsumer mqttConsumer,
        KafkaConsumer kafkaConsumer)
    {
        _logger = logger;
        _config = config;
        _mqttConsumer = mqttConsumer;
        _kafkaConsumer = kafkaConsumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string brokerType = (_config["BROKER_TYPE"] ?? "mqtt").ToLower();
        _logger.LogInformation("Pokretanje Storage servisa. Izabrani broker: {Broker}", brokerType);

        if (brokerType == "mqtt")
        {
            await _mqttConsumer.StartAsync(stoppingToken);

       
            try { await Task.Delay(Timeout.Infinite, stoppingToken); }
            catch (OperationCanceledException) { await _mqttConsumer.FlushRemainingAsync(); }
        }
        else if (brokerType == "kafka")
        {
            await _kafkaConsumer.StartConsumeAsync(stoppingToken);
        }
        else
        {
            _logger.LogCritical("Nepoznat BROKER_TYPE: {Broker}. Servis se gasi.", brokerType);
        }
    }
}