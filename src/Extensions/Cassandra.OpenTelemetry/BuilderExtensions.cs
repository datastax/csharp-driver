//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using Cassandra.Metrics;
using Cassandra.OpenTelemetry.Metrics;

namespace Cassandra.OpenTelemetry
{
    /// <summary>
    /// <see cref="Builder"/> extension methods related to OpenTelemetry implementation.
    /// </summary>
    public static class BuilderExtensions
    {
        /// <summary>
        /// Adds OpenTelemetry instrumentation to the <see cref="Builder"/>.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/>.</param>
        /// <returns></returns>
        public static Builder WithOpenTelemetryInstrumentation(this Builder builder)
        {
            return WithOpenTelemetryInstrumentation(builder, null);
        }

        /// <summary>
        /// Adds OpenTelemetry instrumentation to the <see cref="Builder"/>.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/>.</param>
        /// <param name="options">An action with <see cref="CassandraInstrumentationOptions"/> to be 
        /// included in the instrumentation.</param>
        /// <returns></returns>
        public static Builder WithOpenTelemetryInstrumentation(this Builder builder, Action<CassandraInstrumentationOptions> options)
        {
            var instrumentationOptions = new CassandraInstrumentationOptions();

            options?.Invoke(instrumentationOptions);

            return builder.WithRequestTracker(new OpenTelemetryRequestTracker(instrumentationOptions));
        }

        /// <summary>
        /// Adds OpenTelemetry metrics to the <see cref="Builder"/>.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/>.</param>
        /// <returns>Returning Cassandra builder.</returns>
        public static Builder WithOpenTelemetryMetrics(this Builder builder)
        {
            return builder.WithOpenTelemetryMetrics(null);
        }

        /// <summary>
        /// Adds OpenTelemetry instrumentation to the <see cref="Builder"/>.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/>.</param>
        /// <param name="options">An action with <see cref="CassandraInstrumentationOptions"/> to be 
        /// included in the instrumentation.</param>
        /// <returns></returns>
        public static Builder WithOpenTelemetryMetrics(this Builder builder, Action<DriverMetricsOptions> options)
        {
            var metricsOptions = new DriverMetricsOptions();

            metricsOptions.SetEnabledNodeMetrics(NodeMetric.AllNodeMetrics);
            metricsOptions.SetEnabledSessionMetrics(SessionMetric.AllSessionMetrics);

            options?.Invoke(metricsOptions);

            return builder.WithMetrics(new CassandraDriverMetricsProvider(), metricsOptions);
        }
    }
}
