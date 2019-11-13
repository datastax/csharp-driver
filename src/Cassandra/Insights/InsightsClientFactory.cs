//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.Insights.MessageFactories;
using Cassandra.Insights.Schema.StartupMessage;
using Cassandra.Insights.Schema.StatusMessage;
using Cassandra.SessionManagement;

namespace Cassandra.Insights
{
    internal class InsightsClientFactory : IInsightsClientFactory
    {
        private readonly IInsightsMessageFactory<InsightsStartupData> _startupMessageFactory;
        private readonly IInsightsMessageFactory<InsightsStatusData> _statusMessageFactory;

        public InsightsClientFactory(
            IInsightsMessageFactory<InsightsStartupData> startupMessageFactory,
            IInsightsMessageFactory<InsightsStatusData> statusMessageFactory)
        {
            _startupMessageFactory = startupMessageFactory;
            _statusMessageFactory = statusMessageFactory;
        }

        public IInsightsClient Create(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            return new InsightsClient(cluster, dseSession, _startupMessageFactory, _statusMessageFactory);
        }
    }
}