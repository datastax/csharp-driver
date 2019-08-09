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

using Cassandra.Connections;
using Cassandra.Metrics.DriverAbstractions;
using Cassandra.Metrics.NoopImpl;

namespace Cassandra.Metrics.Registries
{
    internal class HostLevelMetricsRegistry
    {
        public static readonly HostLevelMetricsRegistry EmptyInstance = new HostLevelMetricsRegistry(EmptyDriverMetricsProvider.Instance);

        private readonly IDriverMetricsProvider _driverMetricsProvider;
        public IDriverCounter SpeculativeExecutions { get; }
        public IDriverCounter BytesSent { get; }
        public IDriverCounter BytesReceived { get; }
        public IDriverTimer CqlMessages { get; }
        public IDriverCounter ConnectionInitErrors { get; }
        public IDriverCounter AuthenticationErrors { get; }
        public RequestErrorsLevelMetricsRegistry Errors { get; }
        public RequestErrorsLevelMetricsRegistry Retries { get; }
        public RequestErrorsLevelMetricsRegistry Ignores { get; }

        public HostLevelMetricsRegistry(IDriverMetricsProvider driverMetricsProvider)
        {
            _driverMetricsProvider = driverMetricsProvider;
            SpeculativeExecutions = _driverMetricsProvider.Counter("speculative-executions", DriverMeasurementUnit.Requests);
            BytesSent = _driverMetricsProvider.Counter("bytes-sent", DriverMeasurementUnit.Bytes);
            BytesReceived = _driverMetricsProvider.Counter("bytes-received", DriverMeasurementUnit.Bytes);
            CqlMessages = _driverMetricsProvider.Timer("cql-messages", DriverMeasurementUnit.Requests);

            var connectionErrorsMetricsProvider = _driverMetricsProvider.WithContext("errors").WithContext("connection");
            ConnectionInitErrors = connectionErrorsMetricsProvider.Counter("init", DriverMeasurementUnit.Requests);
            AuthenticationErrors = connectionErrorsMetricsProvider.Counter("auth", DriverMeasurementUnit.Requests);

            Errors = new RequestErrorsLevelMetricsRegistry(_driverMetricsProvider.WithContext("errors").WithContext("request"));
            Retries = new RequestErrorsLevelMetricsRegistry(_driverMetricsProvider.WithContext("retries"));
            Ignores = new RequestErrorsLevelMetricsRegistry(_driverMetricsProvider.WithContext("ignores"));
        }

        public void InitializeHostConnectionPoolGauges(IHostConnectionPool hostConnectionPool)
        {
            // todo (sivukhin, 14.04.2019): Possible <<memory leak>>, because gauges will live until application termination
            var poolDriverMetricsProvider = _driverMetricsProvider.WithContext("pool");
            poolDriverMetricsProvider.Gauge("open-connections",
                () => hostConnectionPool.OpenConnections, DriverMeasurementUnit.None);
            poolDriverMetricsProvider.Gauge("available-streams",
                () => hostConnectionPool.AvailableStreams, DriverMeasurementUnit.None);
            poolDriverMetricsProvider.Gauge("in-flight",
                () => hostConnectionPool.InFlight, DriverMeasurementUnit.None);
            poolDriverMetricsProvider.Gauge("max-requests-per-connection",
                () => hostConnectionPool.MaxRequestsPerConnection, DriverMeasurementUnit.Requests);
        }
    }
}