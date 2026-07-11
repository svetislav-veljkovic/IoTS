using System.Text.Json.Serialization;

namespace AnalyticsService;

public class SensorMessage
{
    [JsonPropertyName("timestamp")]   public DateTime Timestamp { get; set; }
    [JsonPropertyName("device_id")]   public string DeviceId { get; set; } = "";
    [JsonPropertyName("temperature")] public decimal Temperature { get; set; }
    [JsonPropertyName("humidity")]    public decimal Humidity { get; set; }
    [JsonPropertyName("pressure")]    public decimal Pressure { get; set; }
    [JsonPropertyName("light")]       public int Light { get; set; }
    [JsonPropertyName("sound")]       public int Sound { get; set; }
    [JsonPropertyName("motion")]      public int Motion { get; set; }
    [JsonPropertyName("battery")]     public decimal Battery { get; set; }
    [JsonPropertyName("location")]    public string Location { get; set; } = "";
    [JsonPropertyName("ingested_at")] public double? IngestedAt { get; set; }
}
