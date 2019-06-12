// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Linq;
using Dse.SessionManagement;

namespace Dse.Insights
{
    internal class InsightsSupportVerifier : IInsightsSupportVerifier
    {
        private static readonly Version MinDse6Version = new Version(6, 0, 5);
        private static readonly Version MinDse51Version = new Version(5, 1, 13);
        private static readonly Version Dse600Version = new Version(6, 0, 0);

        public bool SupportsInsights(IInternalDseCluster cluster)
        {
            var allHosts = cluster.AllHosts();
            return allHosts.Count != 0 && allHosts.All(h => DseVersionSupportsInsights(h.DseVersion));
        }
        
        public bool DseVersionSupportsInsights(Version dseVersion)
        {
            if (dseVersion == null)
            {
                return false;
            }

            if (dseVersion >= InsightsSupportVerifier.MinDse6Version)
            {
                return true;
            }

            if (dseVersion < InsightsSupportVerifier.Dse600Version)
            {
                if (dseVersion >= InsightsSupportVerifier.MinDse51Version)
                {
                    return true;
                }
            }

            return false;
        }

    }
}