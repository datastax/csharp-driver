// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Cassandra.Insights.Schema;
using Cassandra.SessionManagement;

namespace Cassandra.Insights.MessageFactories
{
    internal interface IInsightsMessageFactory<T>
    {
        Insight<T> CreateMessage(IInternalDseCluster cluster, IInternalDseSession dseSession);
    }
}