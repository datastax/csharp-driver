// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Cassandra.SessionManagement;

namespace Cassandra.Insights.InfoProviders
{
    internal interface IInsightsInfoProvider<out T>
    {
        T GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession);
    }
}