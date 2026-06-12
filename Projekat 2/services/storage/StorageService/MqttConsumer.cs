using System.Buffers;
using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class MqttConsumer
{
    private readonly IConfiguration _config;
    private readonly PostgresRepository _repo;
    private readonly ILogger<MqttConsumer> _logger;
    private IMqttClient? _mqttClient;
    private readonly List<SensorMessage> _batchBuffer = new();
    private readonly int _batchSize;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public MqttConsumer(IConfiguration config, PostgresRepository repo, ILogger<MqttConsumer> logger)
    {
        _config = config;
        _repo = repo;
        _logger = logger;
        _batchSize = int.TryParse(_config["BATCH_SIZE"], out var size) ? size : 500;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_config["MQTT_HOST"] ?? "mosquitto", int.Parse(_config["MQTT_PORT"] ?? "1883"))
            .WithCleanSession()
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
           
            var payloadSegment = e.ApplicationMessage.PayloadSegment;

           
            if (payloadSegment.Count == 0) return;

           
            var payload = Encoding.UTF8.GetString(payloadSegment.Array!, payloadSegment.Offset, payloadSegment.Count);

            try
            {
                var message = JsonSerializer.Deserialize<SensorMessage>(payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (message != null)
                {
                    await _lock.WaitAsync();
                    try
                    {
                        _batchBuffer.Add(message);
                        if (_batchBuffer.Count >= _batchSize)
                        {
                            await _repo.BulkInsertAsync(new List<SensorMessage>(_batchBuffer));
                            _batchBuffer.Clear();
                        }
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Greška pri parsiranju MQTT poruke.");
            }
        };

        await _mqttClient.ConnectAsync(options, cancellationToken);

        var topic = _config["MQTT_TOPIC"] ?? "iot/sensors";

        var subscribeOptions = factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(topic).WithAtLeastOnceQoS())
            .Build();

        await _mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);
        _logger.LogInformation("MqttConsumer uspesno pretplacen na topic: {Topic}", topic);
    }

    public async Task FlushRemainingAsync()
    {
        await _lock.WaitAsync();
        try
        {
            if (_batchBuffer.Count > 0)
            {
                await _repo.BulkInsertAsync(_batchBuffer);
                _batchBuffer.Clear();
            }
        }
        finally
        {
            _lock.Release();
        }
    }
}