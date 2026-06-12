using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class MqttAnalyticsConsumer
{
    private readonly IConfiguration _config;
    private readonly TumblingWindowProcessor _processor;
    private readonly ILogger<MqttAnalyticsConsumer> _logger;
    private IMqttClient? _mqttClient;

    public MqttAnalyticsConsumer(IConfiguration config, TumblingWindowProcessor processor, ILogger<MqttAnalyticsConsumer> logger)
    {
        _config = config;
        _processor = processor;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_config["MQTT_HOST"] ?? "mosquitto", int.Parse(_config["MQTT_PORT"] ?? "1883"))
            .WithCleanSession()
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            var payloadSegment = e.ApplicationMessage.PayloadSegment;
            if (payloadSegment.Count == 0) return Task.CompletedTask;

            var payload = Encoding.UTF8.GetString(payloadSegment.Array!, payloadSegment.Offset, payloadSegment.Count);

            try
            {
                var message = JsonSerializer.Deserialize<SensorMessage>(payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (message != null)
                {
                    _processor.AddMessage(message); 
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Greska pri parsiranju MQTT poruke u analitici.");
            }
            return Task.CompletedTask;
        };

        await _mqttClient.ConnectAsync(options, cancellationToken);

        var topic = _config["MQTT_TOPIC"] ?? "iot/sensors";
        var subscribeOptions = factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(topic).WithAtLeastOnceQoS())
            .Build();

        await _mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);
        _logger.LogInformation("Analitika povezana na MQTT topic: {Topic}", topic);
    }
}