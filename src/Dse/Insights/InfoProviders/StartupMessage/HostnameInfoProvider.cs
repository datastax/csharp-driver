// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Net;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class HostnameInfoProvider : IInsightsInfoProvider<string>
    {
        public string GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            return Dns.GetHostName();
        }
    }
}