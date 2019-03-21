//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Insights;

namespace Dse.SessionManagement
{
    internal class DseSessionManagerFactory : IDseSessionManagerFactory
    {
        private readonly IInsightsClientFactory _insightsClientFactory;

        public DseSessionManagerFactory(IInsightsClientFactory insightsClientFactory)
        {
            _insightsClientFactory = insightsClientFactory;
        }

        public ISessionManager Create(IInternalDseCluster dseCluster, IInternalDseSession dseSession)
        {
            return new DseSessionManager(_insightsClientFactory.Create(dseCluster, dseSession));
        }
    }
}