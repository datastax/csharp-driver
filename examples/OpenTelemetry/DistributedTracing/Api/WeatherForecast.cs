using Cassandra.Mapping.Attributes;

namespace Api
{
    [Table(Keyspace = "weather", Name = "weather_forecast")]
    public class WeatherForecast
    {
        [PartitionKey]
        [Column("id")]
        public Guid Id { get; set; }

        [Column("date")]
        public DateTime? Date { get; set; }

        [Column("temp_c")]
        public int TemperatureC { get; set; }

        [Ignore]
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);

        [Column("summary")]
        public string? Summary { get; set; }
    }
}
