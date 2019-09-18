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

        private readonly IHostConnectionPool _hostConnectionPool;
        
        public NodeMetricsRegistry(
            IDriverMetricsProvider driverMetricsProvider, IHostConnectionPool hostConnectionPool)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _hostConnectionPool = hostConnectionPool;

            InitializeMetrics();
        }

        public IDriverCounter SpeculativeExecutions { get; private set; }

        public IDriverCounter BytesSent { get; private set; }

        public IDriverCounter BytesReceived { get; private set; }

        public IDriverTimer CqlMessages { get; private set; }

        public IDriverCounter ConnectionInitErrors { get; private set; }

        public IDriverCounter AuthenticationErrors { get; private set; }

        public IDriverGauge OpenConnections { get; private set; }

        public IDriverGauge AvailableStreams { get; private set; }

        public IDriverGauge InFlight { get; private set; }

        public IDriverGauge MaxRequestsPerConnection { get; private set; }

        public IRequestMetrics Errors { get; private set; }

        public IRequestMetrics Retries { get; private set; }

        public IRequestMetrics Ignores { get; private set; }

        public IEnumerable<IDriverCounter> Counters { get; private set; }

        public IEnumerable<IDriverGauge> Gauges { get; private set; }

        public IEnumerable<IDriverHistogram> Histograms { get; } = Enumerable.Empty<IDriverHistogram>();

        public IEnumerable<IDriverMeter> Meters { get; private set; }

        public IEnumerable<IDriverTimer> Timers { get; private set; }
        
        public void InitializeMetrics()
        {
            SpeculativeExecutions = _driverMetricsProvider.Counter("speculative-executions", DriverMeasurementUnit.Requests);
            BytesSent = _driverMetricsProvider.Counter("bytes-sent", DriverMeasurementUnit.Bytes);
            BytesReceived = _driverMetricsProvider.Counter("bytes-received", DriverMeasurementUnit.Bytes);
            CqlMessages = _driverMetricsProvider.Timer("cql-messages", DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);

            var connectionErrorsMetricsProvider = _driverMetricsProvider.WithContext("errors").WithContext("connection");
            ConnectionInitErrors = connectionErrorsMetricsProvider.Counter("init", DriverMeasurementUnit.Requests);
            AuthenticationErrors = connectionErrorsMetricsProvider.Counter("auth", DriverMeasurementUnit.Requests);

            Errors = new RequestMetricsRegistry(_driverMetricsProvider.WithContext("errors").WithContext("request"));
            Retries = new RequestMetricsRegistry(_driverMetricsProvider.WithContext("retries"));
            Ignores = new RequestMetricsRegistry(_driverMetricsProvider.WithContext("ignores"));

            var poolDriverMetricsProvider = _driverMetricsProvider.WithContext("pool");
            OpenConnections = poolDriverMetricsProvider.Gauge("open-connections",
                () => _hostConnectionPool.OpenConnections, DriverMeasurementUnit.None);
            AvailableStreams = poolDriverMetricsProvider.Gauge("available-streams",
                () => _hostConnectionPool.AvailableStreams, DriverMeasurementUnit.None);
            InFlight = poolDriverMetricsProvider.Gauge("in-flight",
                () => _hostConnectionPool.InFlight, DriverMeasurementUnit.None);
            MaxRequestsPerConnection = poolDriverMetricsProvider.Gauge("max-requests-per-connection",
                () => _hostConnectionPool.MaxRequestsPerConnection, DriverMeasurementUnit.Requests);

            Counters = new[] { SpeculativeExecutions, BytesSent, BytesReceived, ConnectionInitErrors, AuthenticationErrors };
            Gauges = new[] { OpenConnections, AvailableStreams, InFlight, MaxRequestsPerConnection };
            Meters = Errors.Meters.Concat(Ignores.Meters).Concat(Retries.Meters);
            Timers = new[] { CqlMessages };
        }

        //TODO
        public void Dispose()
        {
            foreach (var gauge in Gauges)
            {
                gauge.Dispose();
            }
        }
    }
}