namespace RestApi.Models
{
    public class SensorReadingDto
    {
        public string DeviceId { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
        public int Co2Level { get; set; }
        public double Voltage { get; set; }
        public double GpsLat { get; set; }
        public double GpsLng { get; set; }
    }

    public class AggregatedDto
    {
        public double AvgTemperature { get; set; }
        public int MaxCo2 { get; set; }
        public double MinVoltage { get; set; }
    }
}
