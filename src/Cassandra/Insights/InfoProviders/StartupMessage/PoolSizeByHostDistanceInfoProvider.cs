// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Cassandra.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;

namespace Cassandra.Insights.InfoProviders.StartupMessage
{
    internal class PoolSizeByHostDistanceInfoProvider : IInsightsInfoProvider<PoolSizeByHostDistance>
    {
        public PoolSizeByHostDistance GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            return new PoolSizeByHostDistance
            {
                Local = cluster
                        .Configuration
                        .CassandraConfiguration
                        .GetPoolingOptions(cluster.Metadata.ControlConnection.ProtocolVersion)
                        .GetCoreConnectionsPerHost(HostDistance.Local),
                Remote = cluster
                         .Configuration
                         .CassandraConfiguration
                         .GetPoolingOptions(cluster.Metadata.ControlConnection.ProtocolVersion)
                         .GetCoreConnectionsPerHost(HostDistance.Remote)
            };
        }
    }
}