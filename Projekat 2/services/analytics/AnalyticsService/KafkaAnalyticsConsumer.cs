using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class KafkaAnalyticsConsumer
{
    private readonly IConfiguration _config;
    private readonly TumblingWindowProcessor _processor;
    private readonly ILogger<KafkaAnalyticsConsumer> _logger;

    public KafkaAnalyticsConsumer(IConfiguration config, TumblingWindowProcessor processor, ILogger<KafkaAnalyticsConsumer> logger)
    {
        _config = config;
        _processor = processor;
        _logger = logger;
    }

    public Task StartConsumeAsync(CancellationToken cancellationToken)
    {

        return Task.Run(() =>
        {
            var conf = new ConsumerConfig
            {
                BootstrapServers = _config["KAFKA_BOOTSTRAP_SERVERS"] ?? "kafka:9092",
               
                GroupId = (_config["KAFKA_CONSUMER_GROUP"] ?? "iot-consumers") + "-analytics",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true 
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            var topic = _config["KAFKA_TOPIC"] ?? "iot-sensors";
            consumer.Subscribe(topic);

            _logger.LogInformation("Analitika povezana na Kafka topic: {Topic}", topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (cr != null)
                        {
                            var message = JsonSerializer.Deserialize<SensorMessage>(cr.Message.Value, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                            if (message != null)
                            {
                                _processor.AddMessage(message); 
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Greska u Kafka konzumer petlji analitike.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }, cancellationToken);
    }
}