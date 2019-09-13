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

using System.Linq;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Providers.Null;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    internal class SessionLevelMetricsRegistry
    {
        public static readonly SessionLevelMetricsRegistry EmptyInstance = new SessionLevelMetricsRegistry(NullDriverMetricsProvider.Instance);
        private readonly IInternalSession _session;
        private readonly IDriverMetricsProvider _driverMetricsProvider;

        public IDriverTimer CqlRequests { get; }
        public IDriverMeter CqlClientTimeouts { get; }
        public IDriverCounter BytesSent { get; }
        public IDriverCounter BytesReceived { get; }

        public SessionLevelMetricsRegistry(IInternalSession session, IDriverMetricsProvider driverMetricsProvider)
        {
            _session = session;
            _driverMetricsProvider = driverMetricsProvider;
            CqlRequests = driverMetricsProvider.Timer("cql-requests", DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);
            CqlClientTimeouts = driverMetricsProvider.Meter("cql-client-timeouts", DriverMeasurementUnit.None);
            BytesSent = driverMetricsProvider.Counter("bytes-sent", DriverMeasurementUnit.Bytes);
            BytesReceived = driverMetricsProvider.Counter("bytes-received", DriverMeasurementUnit.Bytes);
        }

        public void InitializeSessionGauges()
        {
            _driverMetricsProvider.Gauge(
                "connected-nodes", 
                () => _session.GetPools().Count(), //TODO
                DriverMeasurementUnit.None);
        }

        public NodeLevelMetricsRegistry GetNodeLevelMetrics(Host host)
        {
            return new NodeLevelMetricsRegistry(
                _driverMetricsProvider.WithContext("nodes")
                                      .WithContext($"{host.Address.ToString().Replace('.', '_')}")
            );
        }
    }
}