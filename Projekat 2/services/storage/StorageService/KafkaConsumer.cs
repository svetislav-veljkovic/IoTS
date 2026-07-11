using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class KafkaConsumer
{
    private readonly IConfiguration _cfg;
    private readonly PostgresRepository _repo;
    private readonly ILogger<KafkaConsumer> _log;
    private readonly List<SensorMessage> _buf = new();
    private readonly int _batch;
    private readonly bool _on;
    private readonly object _lk = new();
    private DateTime _lastFlush = DateTime.UtcNow;
    private long _invalidJsonCount;
    private DateTime _lastInvalidJsonLog = DateTime.MinValue;
    private static readonly TimeSpan FlushInt = TimeSpan.FromSeconds(2);

    public KafkaConsumer(IConfiguration cfg, PostgresRepository repo, ILogger<KafkaConsumer> log)
    {
        _cfg = cfg; _repo = repo; _log = log;
        _batch = int.TryParse(_cfg["BATCH_SIZE"], out var bs) ? bs : 500;
        _on = !string.Equals(_cfg["STORAGE_ENABLED"], "false", StringComparison.OrdinalIgnoreCase);
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var bs  = _cfg["KAFKA_BOOTSTRAP_SERVERS"] ?? "iot-kafka:9092";
        var grp = _cfg["KAFKA_CONSUMER_GROUP"] ?? "iot-consumers";
        var t   = _cfg["KAFKA_TOPIC"] ?? "iot-sensors";
        using var c = new ConsumerBuilder<Ignore, string>(new ConsumerConfig{
            BootstrapServers = bs, GroupId = grp, AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, AllowAutoCreateTopics = true,
        }).Build();
        c.Subscribe(t);
        _log.LogInformation("Storage Kafka: topic={T} enabled={E} batch={B}", t, _on, _batch);

        var jo = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var cr = c.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr != null)
                    {
                        if (!_on) { CommitSafe(c, cr); continue; }

                      
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
                        }

                        if (m == null) { CommitSafe(c, cr); continue; }

                        List<SensorMessage>? flush = null;
                        lock (_lk)
                        {
                            _buf.Add(m);
                            if (_buf.Count >= _batch || (DateTime.UtcNow - _lastFlush) >= FlushInt)
                            { flush = new List<SensorMessage>(_buf); _buf.Clear(); _lastFlush = DateTime.UtcNow; }
                        }
                        if (flush != null) { await _repo.BulkInsertAsync(flush, ct); CommitSafe(c, cr); }
                    }
                    else if (_on)
                    {
                   
                        List<SensorMessage>? tail = null;
                        lock (_lk) { if (_buf.Count > 0 && (DateTime.UtcNow - _lastFlush) >= FlushInt)
                            { tail = new List<SensorMessage>(_buf); _buf.Clear(); _lastFlush = DateTime.UtcNow; } }
                        if (tail != null)
                        {
                            await _repo.BulkInsertAsync(tail, ct);
                            CommitSafe(c);
                        }
                    }
                }
                catch (ConsumeException ex) { _log.LogError(ex, "Kafka consume greska {R}", ex.Error?.Reason); }
                catch (Exception ex) { _log.LogError(ex, "Neocekivana greska u Kafka petlji, nastavljam."); }
            }
        }
        catch (OperationCanceledException)
        {
            if (_on) { List<SensorMessage> tail; lock (_lk) { tail = new List<SensorMessage>(_buf); _buf.Clear(); }
                if (tail.Count > 0) { await _repo.BulkInsertAsync(tail, CancellationToken.None); CommitSafe(c); } }
            try { c.Close(); } catch { }
        }
    }


    private void CommitSafe(IConsumer<Ignore, string> c, ConsumeResult<Ignore, string> cr)
    {
        try { c.Commit(cr); }
        catch (KafkaException ex) { _log.LogWarning("Kafka commit greska: {R}", ex.Error?.Reason); }
    }


    private void CommitSafe(IConsumer<Ignore, string> c)
    {
        try { c.Commit(); }
        catch (KafkaException ex) { _log.LogWarning("Kafka commit (tail) greska: {R}", ex.Error?.Reason); }
    }
}
