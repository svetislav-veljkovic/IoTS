using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class KafkaAnalyticsConsumer
{
    private readonly IConfiguration _cfg;
    private readonly TumblingWindowProcessor _proc;
    private readonly ILogger<KafkaAnalyticsConsumer> _log;
    private long _invalidJsonCount;
    private DateTime _lastInvalidJsonLog = DateTime.MinValue;

    public KafkaAnalyticsConsumer(IConfiguration cfg, TumblingWindowProcessor proc, ILogger<KafkaAnalyticsConsumer> log)
    { _cfg = cfg; _proc = proc; _log = log; }

    public Task StartAsync(CancellationToken ct) => Task.Run(() =>
    {
        var bs   = _cfg["KAFKA_BOOTSTRAP_SERVERS"] ?? "iot-kafka:9092";
        var grp  = (_cfg["KAFKA_CONSUMER_GROUP"] ?? "iot-consumers") + "-analytics";
        var top  = _cfg["KAFKA_TOPIC"] ?? "iot-sensors";
        var c = new ConsumerBuilder<Ignore, string>(new ConsumerConfig{
            BootstrapServers = bs, GroupId = grp, AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true, AllowAutoCreateTopics = true,
        }).Build();
        c.Subscribe(top);
        _log.LogInformation("Analytics (Kafka): bs={B} topic={T}", bs, top);
        var jo = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var cr = c.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr == null || cr.IsPartitionEOF) continue;

                    
                    SensorMessage? m = null;
                    try { m = JsonSerializer.Deserialize<SensorMessage>(cr.Message.Value, jo); }
                    catch (JsonException ex)
                    {
                        var count = Interlocked.Increment(ref _invalidJsonCount);
                        var now = DateTime.UtcNow;
                        if (count == 1 || now - _lastInvalidJsonLog >= TimeSpan.FromSeconds(10))
                        {
                            _lastInvalidJsonLog = now;
                            _log.LogWarning(
                                "Neispravan Kafka JSON payload, preskacem. Ukupno preskoceno={Count}. Poslednji offset={Offset}. Primer greske: {Msg}",
                                count, cr.Offset, ex.Message);
                        }
                        continue;
                    }

                    if (m != null) _proc.AddMessage(m);
                }
                catch (ConsumeException ex) { _log.LogError(ex, "Kafka consume greska: {R}", ex.Error?.Reason); }
                catch (Exception ex) { _log.LogError(ex, "Neocekivana greska u Analytics Kafka petlji, nastavljam."); }
            }
        }
        catch (OperationCanceledException) { try { c.Close(); } catch { } }
    }, ct);
}
