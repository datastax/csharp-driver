//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections.Generic;
using System.Linq;

namespace Cassandra
{
    /// <summary>
    ///  This policy ensures that any hosts from the provided data centers list 
    ///  will never be used. 
    /// </summary>
    public class BlackListedDCLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private ILoadBalancingPolicy _childPolicy;
        private readonly IList<string> blacklistedDC;

        public BlackListedDCLoadBalancingPolicy(IList<string> blacklistedDC, ILoadBalancingPolicy childPolicy)
        {
            _childPolicy = childPolicy ?? throw new ArgumentException(string.Format("Base child policy wasn't specified"));
            this.blacklistedDC = blacklistedDC;
        }

        public HostDistance Distance(Host host)
        {
            if (blacklistedDC?.Any(x => x?.ToLower() == host.Datacenter.ToLower()) ?? false)
                return HostDistance.Ignored;
            return _childPolicy.Distance(host); ;
        }

        public void Initialize(ICluster cluster)
        {
            _childPolicy.Initialize(cluster);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            return _childPolicy.NewQueryPlan(keyspace, query);
        }
    }
}
