namespace Cassandra.OpenTelemetry
{
    public class CassandraInstrumentationOptions
    {
        public bool IncludeDatabaseStatement { get; set; } = false;
    }
}
