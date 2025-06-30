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

using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.Policies.Util
{
    public class CustomLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private ICluster _cluster;
        private readonly string[] _hosts;

        public CustomLoadBalancingPolicy(string[] hosts)
        {
            _hosts = hosts;
        }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
        }

        public IEnumerable<HostShard> NewQueryPlan(string keyspace, IStatement query)
        {
            var queryPlan = new List<HostShard>();
            var allHosts = _cluster.AllHosts();
            foreach (var host in _hosts)
            {
                queryPlan.Add(new HostShard(allHosts.Single(h => h.Address.ToString() == host), -1));
            }
            return queryPlan;
        }
    }
}
