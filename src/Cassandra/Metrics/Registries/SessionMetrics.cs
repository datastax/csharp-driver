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
using System.Linq;
using Cassandra.Metrics.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    /// <inheritdoc />
    internal class SessionMetrics : ISessionMetrics
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly string _context;

        public SessionMetrics(IDriverMetricsProvider driverMetricsProvider, DriverMetricsOptions metricsOptions, bool metricsEnabled, string context)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _context = context;
            MetricsRegistry = new InternalMetricsRegistry<SessionMetric>(
                driverMetricsProvider, SessionMetric.AllSessionMetrics.Except(metricsOptions.EnabledSessionMetrics), metricsEnabled);
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
                CqlRequests = MetricsRegistry.Timer(_context, SessionMetric.Timers.CqlRequests);
                CqlClientTimeouts = MetricsRegistry.Counter(_context, SessionMetric.Counters.CqlClientTimeouts);
                BytesSent = MetricsRegistry.Meter(_context, SessionMetric.Meters.BytesSent);
                BytesReceived = MetricsRegistry.Meter(_context, SessionMetric.Meters.BytesReceived);
                ConnectedNodes = MetricsRegistry.Gauge(
                    _context, SessionMetric.Gauges.ConnectedNodes, () => session.ConnectedNodes);

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