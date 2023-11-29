using System;
using Cassandra.OpenTelemetry.Implementation;

namespace Cassandra.OpenTelemetry
{
    public static class BuilderExtensions
    {
        public static Builder AddOpenTelemetryInstrumentation(this Builder builder)
        {
            return AddOpenTelemetryInstrumentation(builder, null);
        }

        public static Builder AddOpenTelemetryInstrumentation(this Builder builder, Action<CassandraInstrumentationOptions> options)
        {
            var instrumentationOptions = new CassandraInstrumentationOptions();

            options?.Invoke(instrumentationOptions);

            builder.WithRequestTracker(new Trace(instrumentationOptions));

            return builder;
        }
    }
}
