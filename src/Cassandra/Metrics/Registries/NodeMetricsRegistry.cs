//
//       Copyright (C) 2019 DataStax Inc.
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

using System.Collections.Generic;
using System.Linq;

using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    internal class NodeMetricsRegistry : INodeMetrics
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly string _context;

        private IHostConnectionPool _hostConnectionPool = null;
        
        public NodeMetricsRegistry(IDriverMetricsProvider driverMetricsProvider, string context)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _context = context;

            InitializeMetrics();
        }

        public IDriverCounter SpeculativeExecutions { get; private set; }

        public IDriverCounter BytesSent { get; private set; }

        public IDriverCounter BytesReceived { get; private set; }

        public IDriverTimer CqlMessages { get; private set; }
        
        public IDriverGauge OpenConnections { get; private set; }

        public IDriverGauge AvailableStreams { get; private set; }

        public IDriverGauge InFlight { get; private set; }

        public IDriverGauge MaxRequestsPerConnection { get; private set; }

        public IRequestErrorMetrics Errors { get; private set; }

        public IRetryPolicyMetrics Retries { get; private set; }

        public IRetryPolicyMetrics Ignores { get; private set; }

        public IEnumerable<IDriverCounter> Counters { get; private set; }

        public IEnumerable<IDriverGauge> Gauges { get; private set; }

        public IEnumerable<IDriverHistogram> Histograms { get; } = Enumerable.Empty<IDriverHistogram>();

        public IEnumerable<IDriverMeter> Meters { get; private set; }

        public IEnumerable<IDriverTimer> Timers { get; private set; }

        public IEnumerable<IDriverMetric> All { get; private set; }

        private void InitializeMetrics()
        {
            SpeculativeExecutions = _driverMetricsProvider.Counter(_context, "speculative-executions", DriverMeasurementUnit.Requests);
            BytesSent = _driverMetricsProvider.Counter(_context, "bytes-sent", DriverMeasurementUnit.Bytes);
            BytesReceived = _driverMetricsProvider.Counter(_context, "bytes-received", DriverMeasurementUnit.Bytes);
            CqlMessages = _driverMetricsProvider.Timer(_context, "cql-messages", DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);
            
            Errors = new RequestErrorMetricsRegistry(_driverMetricsProvider, _context, "errors.");
            Retries = new RetryPolicyMetrics(_driverMetricsProvider, _context, "retries.");
            Ignores = new RetryPolicyMetrics(_driverMetricsProvider, _context, "ignores.");
            
            OpenConnections = _driverMetricsProvider.Gauge(_context, "pool.open-connections",
                () => _hostConnectionPool?.OpenConnections, DriverMeasurementUnit.None);
            AvailableStreams = _driverMetricsProvider.Gauge(_context, "pool.available-streams",
                () => _hostConnectionPool?.AvailableStreams, DriverMeasurementUnit.None);
            InFlight = _driverMetricsProvider.Gauge(_context, "pool.in-flight",
                () => _hostConnectionPool?.InFlight, DriverMeasurementUnit.None);
            MaxRequestsPerConnection = _driverMetricsProvider.Gauge(_context, "pool.max-requests-per-connection",
                () => _hostConnectionPool?.MaxRequestsPerConnection, DriverMeasurementUnit.Requests);

            Counters = Errors.Counters.Concat(new[] { SpeculativeExecutions, BytesSent, BytesReceived });
            Gauges = new[] { OpenConnections, AvailableStreams, InFlight, MaxRequestsPerConnection };
            Meters = Errors.Meters.Concat(Ignores.Meters).Concat(Retries.Meters);
            Timers = new[] { CqlMessages };
            All = new IDriverMetric[0].Concat(Counters).Concat(Gauges).Concat(Histograms).Concat(Meters).Concat(Timers);
        }

        public void InitializePoolGauges(IHostConnectionPool pool)
        {
            _hostConnectionPool = pool;
        }

        public void Dispose()
        {
            _driverMetricsProvider.ShutdownMetricsContext(_context);
        }
    }
}