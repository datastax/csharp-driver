// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class DataCentersInfoProvider : IInsightsInfoProvider<HashSet<string>>
    {
        public HashSet<string> GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var dataCenters = new HashSet<string>();
            var remoteConnectionsLength =
                cluster
                    .Configuration
                    .CassandraConfiguration
                    .GetPoolingOptions(cluster.Metadata.ControlConnection.ProtocolVersion)
                    .GetCoreConnectionsPerHost(HostDistance.Remote);

            foreach (var h in cluster.AllHosts()) 
            {
                if (h.Datacenter == null)
                {
                    continue;
                }

                var distance = cluster.Configuration.CassandraConfiguration.Policies.LoadBalancingPolicy.Distance(h);
                if (distance == HostDistance.Local || (distance == HostDistance.Remote && remoteConnectionsLength > 0))
                {
                    dataCenters.Add(h.Datacenter);
                }
            }

            return dataCenters;
        }
    }
}