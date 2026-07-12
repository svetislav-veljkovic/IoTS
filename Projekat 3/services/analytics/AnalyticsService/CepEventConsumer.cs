using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace AnalyticsService;

public class CepEventConsumer
{
    private readonly IConfiguration _cfg;
    private readonly ILogger<CepEventConsumer> _log;
    private readonly string _conn;
    private IMqttClient? _client;

    public CepEventConsumer(IConfiguration cfg, ILogger<CepEventConsumer> log)
    {
        _cfg = cfg;
        _log = log;
        _conn = cfg["POSTGRES_CONNECTION_STRING"]
            ?? "Host=iot-postgres;Port=5432;Database=iot_data;Username=iotuser;Password=iotpassword";
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var host = _cfg["MQTT_HOST"] ?? "iot-mosquitto";
        var port = int.TryParse(_cfg["MQTT_PORT"], out var pp) ? pp : 1883;
        var topic = _cfg["MQTT_CEP_TOPIC"] ?? "iot/analytics/events";
        var qos = int.TryParse(_cfg["MQTT_QOS"], out var q) ? Math.Clamp(q, 0, 2) : 1;

        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(host, port)
            .WithCleanSession(false)
            .WithClientId(_cfg["MQTT_CEP_CLIENT_ID"] ?? "analytics-cep-svc")
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
            .Build();

        _client.ApplicationMessageReceivedAsync += async e =>
        {
            var payload = e.ApplicationMessage.PayloadSegment;
            if (payload.Count == 0) return;
            var json = Encoding.UTF8.GetString(payload.Array!, payload.Offset, payload.Count);
            try
            {
                await SaveEventAsync(json, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Greska pri obradi eKuiper CEP eventa.");
            }
        };

        _client.DisconnectedAsync += async _ =>
        {
            _log.LogWarning("MQTT diskonekcija CEP consumer-a, reconnect...");
            try
            {
                await Task.Delay(5000, ct);
                if (!ct.IsCancellationRequested && _client != null)
                {
                    await _client.ConnectAsync(options, ct);
                    await SubscribeAsync(topic, qos, ct);
                }
            }
            catch (OperationCanceledException) { }
        };

        await _client.ConnectAsync(options, ct);
        await SubscribeAsync(topic, qos, ct);
        _log.LogInformation("Analytics CEP consumer pretplacen na {Topic}", topic);
    }

    private async Task SubscribeAsync(string topic, int qos, CancellationToken ct)
    {
        if (_client == null) return;
        var options = new MqttFactory().CreateSubscribeOptionsBuilder()
            .WithTopicFilter(x => x.WithTopic(topic).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos))
            .Build();
        await _client.SubscribeAsync(options, ct);
    }

    private async Task SaveEventAsync(string json, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement.ValueKind == JsonValueKind.Array && doc.RootElement.GetArrayLength() > 0
            ? doc.RootElement[0]
            : doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            _log.LogWarning("eKuiper CEP payload nije objekat: {Payload}", json);
            return;
        }
        var eventType = GetString(root, "event_type") ?? "ekuiper_event";
        var avgTemp = GetDouble(root, "avg_temperature");
        var maxTemp = GetDouble(root, "max_temperature");
        var avgHum = GetDouble(root, "avg_humidity");
        var count = GetInt(root, "message_count");
        var severity = maxTemp >= 70 || avgTemp >= 60 ? "critical" : "warning";

        const string sql = @"
            INSERT INTO cep_events
                (event_type, source, message_count, avg_temperature, max_temperature,
                 avg_humidity, severity, payload)
            VALUES ($1,'ekuiper',$2,$3,$4,$5,$6,$7::jsonb)";

        await using var c = new NpgsqlConnection(_conn);
        await c.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, c);
        cmd.Parameters.AddWithValue(eventType);
        cmd.Parameters.AddWithValue((object?)count ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)avgTemp ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)maxTemp ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)avgHum ?? DBNull.Value);
        cmd.Parameters.AddWithValue(severity);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Jsonb, json);
        await cmd.ExecuteNonQueryAsync(ct);

        _log.LogWarning("eKuiper CEP event: type={Type} severity={Severity} avgTemp={Avg:F2} maxTemp={Max:F2} count={Count}",
            eventType, severity, avgTemp, maxTemp, count);
    }

    private static string? GetString(JsonElement root, string name) =>
        root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static double? GetDouble(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var v)) return null;
        if (v.ValueKind == JsonValueKind.Number && v.TryGetDouble(out var d)) return d;
        if (v.ValueKind == JsonValueKind.String && double.TryParse(v.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out d)) return d;
        return null;
    }

    private static int? GetInt(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var v)) return null;
        if (v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i)) return i;
        if (v.ValueKind == JsonValueKind.String && int.TryParse(v.GetString(), out i)) return i;
        return null;
    }
}
