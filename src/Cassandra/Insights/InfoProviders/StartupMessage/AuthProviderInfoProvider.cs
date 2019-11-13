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