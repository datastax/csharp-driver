//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

using Dse.Insights.Schema.StatusMessage;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StatusMessage
{
    internal class NodeStatusInfoProvider : IInsightsInfoProvider<Dictionary<string, NodeStatusInfo>>
    {
        public Dictionary<string, NodeStatusInfo> GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var nodeStatusDictionary = new Dictionary<string, NodeStatusInfo>();
            var state = dseSession.GetState();
            var connectedHosts = state.GetConnectedHosts();

            foreach (var h in connectedHosts)
            {
                var inFlightQueries = state.GetInFlightQueries(h);
                var openConnections = state.GetOpenConnections(h);
                nodeStatusDictionary.Add(
                    h.Address.ToString(),
                    new NodeStatusInfo
                    {
                        Connections = openConnections,
                        InFlightQueries = inFlightQueries
                    });
            }

            return nodeStatusDictionary;
        }
    }
}