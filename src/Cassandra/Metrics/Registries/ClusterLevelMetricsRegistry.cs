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
using Cassandra.Metrics.DriverAbstractions;
using Cassandra.Metrics.NoopImpl;

namespace Cassandra.Metrics.Registries
{
    internal class ClusterLevelMetricsRegistry
    {
        public static readonly ClusterLevelMetricsRegistry EmptyInstance = new ClusterLevelMetricsRegistry(EmptyDriverMetricsProvider.Instance);
        private readonly IDriverMetricsProvider _driverMetricsProvider;

        public IDriverTimer CqlRequests { get; }
        public IDriverMeter CqlClientTimeouts { get; }
        public IDriverCounter BytesSent { get; }
        public IDriverCounter BytesReceived { get; }

        public ClusterLevelMetricsRegistry(IDriverMetricsProvider driverMetricsProvider)
        {
            _driverMetricsProvider = driverMetricsProvider;
            CqlRequests = driverMetricsProvider.Timer("cql-requests", DriverMeasurementUnit.Requests);
            CqlClientTimeouts = driverMetricsProvider.Meter("cql-client-timeouts", DriverMeasurementUnit.None);
            BytesSent = driverMetricsProvider.Counter("bytes-sent", DriverMeasurementUnit.Bytes);
            BytesReceived = driverMetricsProvider.Counter("bytes-received", DriverMeasurementUnit.Bytes);
        }

        public void InitializeClusterGauges(Cluster cluster)
        {
            _driverMetricsProvider.Gauge(
                "alive-hosts", () => cluster.Metadata.Hosts.Count(host => host.IsUp), DriverMeasurementUnit.None);
            _driverMetricsProvider.Gauge(
                "connected-hosts", () => cluster.Metadata.Hosts.Count(), DriverMeasurementUnit.None);
        }

        public HostLevelMetricsRegistry GetHostLevelMetrics(Host host)
        {
            return new HostLevelMetricsRegistry(
                _driverMetricsProvider.WithContext("nodes")
                                      .WithContext($"{host.Address.ToString().Replace('.', '_')}")
            );
        }
    }
}