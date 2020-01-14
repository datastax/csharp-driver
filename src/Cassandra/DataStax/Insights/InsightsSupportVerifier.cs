//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// 

using System;
using System.Linq;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights
{
    internal class InsightsSupportVerifier : IInsightsSupportVerifier
    {
        private static readonly Version MinDse6Version = new Version(6, 0, 5);
        private static readonly Version MinDse51Version = new Version(5, 1, 13);
        private static readonly Version Dse600Version = new Version(6, 0, 0);

        public bool SupportsInsights(IInternalCluster cluster)
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