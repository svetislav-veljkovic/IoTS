using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly TumblingWindowProcessor _processor;
    private readonly MqttAnalyticsConsumer _mqttConsumer;
    private readonly KafkaAnalyticsConsumer _kafkaConsumer;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration config,
        TumblingWindowProcessor processor,
        MqttAnalyticsConsumer mqttConsumer,
        KafkaAnalyticsConsumer kafkaConsumer)
    {
        _logger = logger;
        _config = config;
        _processor = processor;
        _mqttConsumer = mqttConsumer;
        _kafkaConsumer = kafkaConsumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string brokerType = (_config["BROKER_TYPE"] ?? "mqtt").ToLower();
        _logger.LogInformation("Pokretanje Analytics servisa. Izabrani broker: {Broker}", brokerType);


        _processor.Start();

        if (brokerType == "mqtt")
        {
            await _mqttConsumer.StartAsync(stoppingToken);
      
            await Task.Delay(Timeout.Infinite, stoppingToken);
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