// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class OtherOptionsInfoProvider : IInsightsInfoProvider<Dictionary<string, object>>
    {
        public Dictionary<string, object> GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            return null;
        }
    }
}