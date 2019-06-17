// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Dse.SessionManagement;

namespace Dse.Insights
{
    internal interface IInsightsClientFactory
    {
        IInsightsClient Create(IInternalDseCluster cluster, IInternalDseSession dseSession);
    }
}