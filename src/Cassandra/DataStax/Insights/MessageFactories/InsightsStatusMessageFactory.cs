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
using Cassandra.DataStax.Insights.InfoProviders;
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.MessageFactories
{
    internal class InsightsStatusMessageFactory : IInsightsMessageFactory<InsightsStatusData>
    {
        private const string StatusMessageName = "driver.status";
        private const string StatusV1MappingId = "v1";

        private readonly IInsightsMetadataFactory _insightsMetadataFactory;
        private readonly IInsightsInfoProvider<Dictionary<string, NodeStatusInfo>> _connectedNodesInfoProvider;

        public InsightsStatusMessageFactory(
            IInsightsMetadataFactory insightsMetadataFactory,
            IInsightsInfoProvider<Dictionary<string, NodeStatusInfo>> connectedNodesInfoProvider)
        {
            _insightsMetadataFactory = insightsMetadataFactory;
            _connectedNodesInfoProvider = connectedNodesInfoProvider;
        }

        public Insight<InsightsStatusData> CreateMessage(IInternalCluster cluster, IInternalSession session)
        {
            var metadata = _insightsMetadataFactory.CreateInsightsMetadata(
                InsightsStatusMessageFactory.StatusMessageName, InsightsStatusMessageFactory.StatusV1MappingId, InsightType.Event);

            var data = new InsightsStatusData
            {
                ClientId = cluster.Configuration.ClusterId.ToString(),
                SessionId = session.InternalSessionId.ToString(),
                ControlConnection = cluster.Metadata.ControlConnection.EndPoint?.GetHostIpEndPointWithFallback().ToString(),
                ConnectedNodes = _connectedNodesInfoProvider.GetInformation(cluster, session)
            };

            return new Insight<InsightsStatusData>
            {
                Metadata = metadata,
                Data = data
            };
        }
    }
}