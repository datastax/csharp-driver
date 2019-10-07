//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;

using Cassandra.Metrics.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    /// <inheritdoc />
    internal class SessionMetrics : ISessionMetrics
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly string _context;

        public SessionMetrics(IDriverMetricsProvider driverMetricsProvider, MetricsOptions metricsOptions, bool metricsEnabled, string context)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _context = context;
            MetricsRegistry = new InternalMetricsRegistry<SessionMetric>(driverMetricsProvider, metricsOptions.DisabledSessionMetrics, metricsEnabled);
        }

        public IDriverTimer CqlRequests { get; private set; }

        public IDriverCounter CqlClientTimeouts { get; private set; }

        public IDriverMeter BytesSent { get; private set; }

        public IDriverMeter BytesReceived { get; private set; }

        public IDriverGauge ConnectedNodes { get; private set; }

        /// <inheritdoc />
        public IInternalMetricsRegistry<SessionMetric> MetricsRegistry { get; }

        public void InitializeMetrics(IInternalSession session)
        {
            try
            {
                CqlRequests = MetricsRegistry.Timer(_context, SessionMetric.Timers.CqlRequests, DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);
                CqlClientTimeouts = MetricsRegistry.Counter(_context, SessionMetric.Counters.CqlClientTimeouts, DriverMeasurementUnit.None);
                BytesSent = MetricsRegistry.Meter(_context, SessionMetric.Meters.BytesSent, DriverMeasurementUnit.Bytes);
                BytesReceived = MetricsRegistry.Meter(_context, SessionMetric.Meters.BytesReceived, DriverMeasurementUnit.Bytes);
                ConnectedNodes = MetricsRegistry.Gauge(
                    _context, SessionMetric.Gauges.ConnectedNodes, () => session.NumberOfConnectionPools, DriverMeasurementUnit.None);

                MetricsRegistry.OnMetricsAdded();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            _driverMetricsProvider.ShutdownMetricsBucket(_context);
        }
    }
}