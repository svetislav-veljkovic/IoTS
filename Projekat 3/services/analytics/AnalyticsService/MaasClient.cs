using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AnalyticsService;

public class MaasClient
{
    private readonly HttpClient _http;
    private readonly ILogger<MaasClient> _logger;
    private readonly string _baseUrl;

    public MaasClient(HttpClient http, IConfiguration config, ILogger<MaasClient> logger)
    {
        _http = http;
        _logger = logger;
        _baseUrl = (config["MAAS_URL"] ?? "http://maas:8000").TrimEnd('/');
    }

    public async Task<MaasPredictionResponse?> PredictAsync(MaasPredictionRequest request, CancellationToken ct)
    {
        try
        {
            using var response = await _http.PostAsJsonAsync($"{_baseUrl}/predict/window", request, ct);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("MaaS vratio status {Status}", response.StatusCode);
                return null;
            }
            return await response.Content.ReadFromJsonAsync<MaasPredictionResponse>(cancellationToken: ct);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            _logger.LogWarning(ex, "MaaS nije dostupan za predikciju.");
            return null;
        }
    }
}

public class MaasPredictionRequest
{
    [JsonPropertyName("message_count")] public int MessageCount { get; set; }
    [JsonPropertyName("avg_temperature")] public double AvgTemperature { get; set; }
    [JsonPropertyName("max_temperature")] public double MaxTemperature { get; set; }
    [JsonPropertyName("min_temperature")] public double MinTemperature { get; set; }
    [JsonPropertyName("avg_humidity")] public double AvgHumidity { get; set; }
    [JsonPropertyName("avg_battery")] public double AvgBattery { get; set; }
    [JsonPropertyName("motion_ratio")] public double MotionRatio { get; set; }
    [JsonPropertyName("window_seconds")] public int WindowSeconds { get; set; }
}

public class MaasPredictionResponse
{
    [JsonPropertyName("risk_level")] public string RiskLevel { get; set; } = "unknown";
    [JsonPropertyName("risk_score")] public double RiskScore { get; set; }
    [JsonPropertyName("model_name")] public string ModelName { get; set; } = "";
}
