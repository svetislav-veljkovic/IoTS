using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class KafkaConsumer
{
    private readonly IConfiguration _config;
    private readonly PostgresRepository _repo;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly List<SensorMessage> _batchBuffer = new();
    private readonly int _batchSize;

    public KafkaConsumer(IConfiguration config, PostgresRepository repo, ILogger<KafkaConsumer> logger)
    {
        _config = config;
        _repo = repo;
        _logger = logger;
        _batchSize = int.TryParse(_config["BATCH_SIZE"], out var size) ? size : 500;
    }

    public async Task StartConsumeAsync(CancellationToken cancellationToken)
    {
        var conf = new ConsumerConfig
        {
            BootstrapServers = _config["KAFKA_BOOTSTRAP_SERVERS"] ?? "kafka:9092",
            GroupId = _config["KAFKA_CONSUMER_GROUP"] ?? "iot-consumers",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false 
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
        var topic = _config["KAFKA_TOPIC"] ?? "iot-sensors";
        consumer.Subscribe(topic);

        _logger.LogInformation("KafkaConsumer uspesno pretplacen na topic: {Topic}", topic);

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
                            _batchBuffer.Add(message);
                            if (_batchBuffer.Count >= _batchSize)
                            {
                                await _repo.BulkInsertAsync(_batchBuffer);
                                consumer.Commit(cr);
                                _batchBuffer.Clear();
                            }
                        }
                    }
                    else if (_batchBuffer.Count > 0)
                    {
                        
                        await _repo.BulkInsertAsync(_batchBuffer);
                        _batchBuffer.Clear();
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Greska u Kafka konzumer petlji.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            if (_batchBuffer.Count > 0)
            {
                await _repo.BulkInsertAsync(_batchBuffer);
            }
            consumer.Close();
        }
    }
}