//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

using Dse.Insights.InfoProviders;
using Dse.Insights.Schema;
using Dse.Insights.Schema.StatusMessage;
using Dse.SessionManagement;

namespace Dse.Insights.MessageFactories
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

        public Insight<InsightsStatusData> CreateMessage(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var metadata = _insightsMetadataFactory.CreateInsightsMetadata(
                InsightsStatusMessageFactory.StatusMessageName, InsightsStatusMessageFactory.StatusV1MappingId, InsightType.Event);

            var data = new InsightsStatusData
            {
                ClientId = cluster.Configuration.ClusterId.ToString(),
                SessionId = dseSession.InternalSessionId.ToString(),
                ControlConnection = cluster.Metadata.ControlConnection.Address?.ToString(),
                ConnectedNodes = _connectedNodesInfoProvider.GetInformation(cluster, dseSession)
            };

            return new Insight<InsightsStatusData>
            {
                Metadata = metadata,
                Data = data
            };
        }
    }
}