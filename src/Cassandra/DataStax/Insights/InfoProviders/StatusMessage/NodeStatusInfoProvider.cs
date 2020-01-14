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

using System.Collections.Generic;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.InfoProviders.StatusMessage
{
    internal class NodeStatusInfoProvider : IInsightsInfoProvider<Dictionary<string, NodeStatusInfo>>
    {
        public Dictionary<string, NodeStatusInfo> GetInformation(IInternalCluster cluster, IInternalSession session)
        {
            var nodeStatusDictionary = new Dictionary<string, NodeStatusInfo>();
            var state = session.GetState();
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