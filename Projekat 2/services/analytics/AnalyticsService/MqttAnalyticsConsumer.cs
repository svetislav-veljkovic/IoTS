using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class MqttAnalyticsConsumer
{
    private readonly IConfiguration _cfg;
    private readonly TumblingWindowProcessor _proc;
    private readonly ILogger<MqttAnalyticsConsumer> _log;
    private readonly int _qos;
    private IMqttClient? _c;

    public MqttAnalyticsConsumer(IConfiguration cfg, TumblingWindowProcessor proc, ILogger<MqttAnalyticsConsumer> log)
    {
        _cfg = cfg; _proc = proc; _log = log;
        _qos = int.TryParse(_cfg["MQTT_QOS"], out var qos) ? Math.Clamp(qos, 0, 2) : 1;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var host  = _cfg["MQTT_HOST"] ?? "iot-mosquitto";
        var port  = int.TryParse(_cfg["MQTT_PORT"], out var pp) ? pp : 1883;
        var topic = _cfg["MQTT_TOPIC"] ?? "iot/sensors";
        var clientId = _cfg["MQTT_CLIENT_ID"] ?? "analytics-svc";
        var cleanSession = bool.TryParse(_cfg["MQTT_CLEAN_SESSION"], out var cs) ? cs : false;

        var f = new MqttFactory();
        _c = f.CreateMqttClient();
        var opt = new MqttClientOptionsBuilder()
            .WithTcpServer(host, port).WithCleanSession(cleanSession)
            .WithClientId(clientId).WithKeepAlivePeriod(TimeSpan.FromSeconds(30)).Build();

        var jo = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        _c.ApplicationMessageReceivedAsync += e =>
        {
            var ps = e.ApplicationMessage.PayloadSegment;
            if (ps.Count == 0) return Task.CompletedTask;
            var s = Encoding.UTF8.GetString(ps.Array!, ps.Offset, ps.Count);
            try
            {
                var m = JsonSerializer.Deserialize<SensorMessage>(s, jo);
                if (m != null) _proc.AddMessage(m);
            }
            catch (Exception ex) { _log.LogError(ex, "MQTT parse greska u analytics."); }
            return Task.CompletedTask;
        };
        _c.DisconnectedAsync += async _ =>
        {
            _log.LogWarning("MQTT diskonekcija analytics, reconnect...");
            try { await Task.Delay(5000, ct); if (!ct.IsCancellationRequested && _c != null) { await _c.ConnectAsync(opt, ct); await Sub(topic, ct); } }
            catch (OperationCanceledException) { }
        };
        await _c.ConnectAsync(opt, ct);
        await Sub(topic, ct);
    }
    private async Task Sub(string t, CancellationToken ct)
    {
        if (_c == null) return;
        var f = new MqttFactory();
        var qos = (MqttQualityOfServiceLevel)_qos;
        var so = f.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(x => x.WithTopic($"{t}/#").WithQualityOfServiceLevel(qos)).Build();
        await _c.SubscribeAsync(so, ct);
        _log.LogInformation("Analytics (MQTT) pretplacen na {T}/# qos={Q} clientId={ClientId}", t, _qos, _cfg["MQTT_CLIENT_ID"] ?? "analytics-svc");
    }
}
