// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Dse.Insights.Schema;
using Dse.SessionManagement;

namespace Dse.Insights.MessageFactories
{
    internal interface IInsightsMessageFactory<T>
    {
        Insight<T> CreateMessage(IInternalDseCluster cluster, IInternalDseSession dseSession);
    }
}