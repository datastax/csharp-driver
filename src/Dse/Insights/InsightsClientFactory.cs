//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Insights.MessageFactories;
using Dse.Insights.Schema.StartupMessage;
using Dse.Insights.Schema.StatusMessage;
using Dse.SessionManagement;

namespace Dse.Insights
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