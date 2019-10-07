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

using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    /// <inheritdoc />
    internal class NodeMetrics : INodeMetrics
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly string _context;

        private IHostConnectionPool _hostConnectionPool = null;

        public NodeMetrics(IDriverMetricsProvider driverMetricsProvider, MetricsOptions metricOptions, string context)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _context = context;
            MetricsRegistry = new InternalMetricsRegistry<NodeMetric>(driverMetricsProvider, metricOptions.DisabledNodeMetrics);

            InitializeMetrics();
        }

        public IDriverCounter SpeculativeExecutions { get; private set; }

        public IDriverMeter BytesSent { get; private set; }

        public IDriverMeter BytesReceived { get; private set; }

        public IDriverTimer CqlMessages { get; private set; }

        public IDriverGauge OpenConnections { get; private set; }

        public IDriverGauge InFlight { get; private set; }

        public IRequestErrorMetrics Errors { get; private set; }

        public IRetryPolicyMetrics Retries { get; private set; }

        public IRetryPolicyMetrics Ignores { get; private set; }

        /// <inheritdoc />
        public IInternalMetricsRegistry<NodeMetric> MetricsRegistry { get; }

        private void InitializeMetrics()
        {
            try
            {
                SpeculativeExecutions = MetricsRegistry.Counter(
                    _context, NodeMetric.Counters.SpeculativeExecutions, DriverMeasurementUnit.Requests);
                BytesSent = MetricsRegistry.Meter(_context, NodeMetric.Meters.BytesSent, DriverMeasurementUnit.Bytes);
                BytesReceived = MetricsRegistry.Meter(_context, NodeMetric.Meters.BytesReceived, DriverMeasurementUnit.Bytes);
                CqlMessages = MetricsRegistry.Timer(
                    _context, NodeMetric.Timers.CqlMessages, DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);

                Errors = new RequestErrorMetrics(MetricsRegistry, _context);
                Retries = new RetryPolicyOnRetryMetrics(MetricsRegistry, _context);
                Ignores = new RetryPolicyOnIgnoreMetrics(MetricsRegistry, _context);

                OpenConnections = MetricsRegistry.Gauge(
                    _context, NodeMetric.Gauges.OpenConnections, () => _hostConnectionPool?.OpenConnections, DriverMeasurementUnit.None);
                
                InFlight = MetricsRegistry.Gauge(
                    _context, NodeMetric.Gauges.InFlight, () => _hostConnectionPool?.InFlight, DriverMeasurementUnit.None);

                MetricsRegistry.OnMetricsAdded();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc />
        public void InitializePoolGauges(IHostConnectionPool pool)
        {
            _hostConnectionPool = pool;
        }

        public void Dispose()
        {
            _driverMetricsProvider.ShutdownMetricsBucket(_context);
        }
    }
}