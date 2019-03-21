// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Dse.Insights.Schema.StartupMessage;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class AuthProviderInfoProvider : IInsightsInfoProvider<AuthProviderInfo>
    {
        public AuthProviderInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var type = cluster.Configuration.CassandraConfiguration.AuthProvider.GetType();
            return new AuthProviderInfo
            {
                Namespace = type.Namespace,
                Type = type.Name
            };
        }
    }
}