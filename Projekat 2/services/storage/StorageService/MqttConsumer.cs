using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StorageService;

public class MqttConsumer
{
    private readonly IConfiguration _cfg;
    private readonly PostgresRepository _repo;
    private readonly ILogger<MqttConsumer> _log;
    private IMqttClient? _c;
    private readonly List<SensorMessage> _buf = new();
    private readonly int _batch;
    private readonly int _qos;
    private readonly bool _on;
    private readonly SemaphoreSlim _lk = new(1, 1);
    private DateTime _lastFlush = DateTime.UtcNow;
    private static readonly TimeSpan FlushInt = TimeSpan.FromSeconds(2);

    public MqttConsumer(IConfiguration cfg, PostgresRepository repo, ILogger<MqttConsumer> log)
    { _cfg = cfg; _repo = repo; _log = log;
      _batch = int.TryParse(_cfg["BATCH_SIZE"], out var bs) ? bs : 500;
      _qos = int.TryParse(_cfg["MQTT_QOS"], out var qos) ? Math.Clamp(qos, 0, 2) : 1;
      _on = !string.Equals(_cfg["STORAGE_ENABLED"], "false", StringComparison.OrdinalIgnoreCase); }

    public async Task StartAsync(CancellationToken ct)
    {
        var h = _cfg["MQTT_HOST"] ?? "iot-mosquitto";
        var p = int.TryParse(_cfg["MQTT_PORT"], out var pp) ? pp : 1883;
        var t = _cfg["MQTT_TOPIC"] ?? "iot/sensors";
        var f = new MqttFactory(); _c = f.CreateMqttClient();
        var opt = new MqttClientOptionsBuilder().WithTcpServer(h,p)
            .WithCleanSession(false).WithClientId("storage-svc").WithKeepAlivePeriod(TimeSpan.FromSeconds(30)).Build();
        var jo = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        _ = Task.Run(async () => {
            using var tm = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
            try { while (await tm.WaitForNextTickAsync(ct)) await FlushIfNeeded(false, ct); }
            catch (OperationCanceledException) { }
        }, ct);

        _c.ApplicationMessageReceivedAsync += async e =>
        {
            var ps = e.ApplicationMessage.PayloadSegment; if (ps.Count == 0) return;
            var s = Encoding.UTF8.GetString(ps.Array!, ps.Offset, ps.Count);
            try
            {
                var m = JsonSerializer.Deserialize<SensorMessage>(s, jo); if (m == null || !_on) return;
                List<SensorMessage>? flush = null;
                await _lk.WaitAsync(ct);
                try { _buf.Add(m);
                    if (_buf.Count >= _batch) { flush = new List<SensorMessage>(_buf); _buf.Clear(); _lastFlush = DateTime.UtcNow; } }
                finally { _lk.Release(); }
                if (flush != null) await _repo.BulkInsertAsync(flush, ct);
            }
            catch (Exception ex) { _log.LogError(ex, "MQTT greska."); }
        };
        _c.DisconnectedAsync += async _ =>
        {
            _log.LogWarning("MQTT diskonekcija storage, reconnect...");
            try { await Task.Delay(5000, ct); if (!ct.IsCancellationRequested && _c != null) { await _c.ConnectAsync(opt, ct); await Sub(t, ct); } }
            catch (OperationCanceledException) { }
        };
        await _c.ConnectAsync(opt, ct); await Sub(t, ct);
        _log.LogInformation("Storage MQTT: {T}/# enabled={E} batch={B} qos={Q}", t, _on, _batch, _qos);
    }

    private async Task Sub(string t, CancellationToken ct)
    {
        if (_c == null) return;
        var f = new MqttFactory();
        var qos = (MqttQualityOfServiceLevel)_qos;
        var so = f.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(b => b.WithTopic($"{t}/#").WithQualityOfServiceLevel(qos))
            .Build();
        await _c.SubscribeAsync(so, ct);
    }

    private async Task FlushIfNeeded(bool force, CancellationToken ct)
    {
        if (!_on) return;
        List<SensorMessage>? flush = null;
        await _lk.WaitAsync(ct);
        try { if (_buf.Count == 0) return;
            if (!force && (DateTime.UtcNow - _lastFlush) < FlushInt) return;
            flush = new List<SensorMessage>(_buf); _buf.Clear(); _lastFlush = DateTime.UtcNow; }
        finally { _lk.Release(); }
        if (flush != null && flush.Count > 0) await _repo.BulkInsertAsync(flush, ct);
    }

    public async Task FlushRemaining()
    {
        await _lk.WaitAsync();
        try { if (_buf.Count > 0 && _on) { await _repo.BulkInsertAsync(new List<SensorMessage>(_buf), CancellationToken.None); _buf.Clear(); } }
        finally { _lk.Release(); }
    }
}
