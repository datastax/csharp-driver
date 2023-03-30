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
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// A load balancing policy designed to run against both DSE and Apache Cassandra clusters.
    /// <para>
    ///  For most workloads, the query plan will be determined in a similar way to TokenAwarePolicy(DCAwareRoundRobinPolicy).
    /// </para>
    /// <para>
    ///  For graph analytics queries, this policy sets the preferred analytics graph server
    ///  previously obtained by driver as the first host in the query plan. After this host, the query plan is the same as the one
    /// returned for other workloads.
    /// </para>
    /// </summary>
    public class DefaultLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private volatile Host _lastPreferredHost;

        /// <summary>
        /// Creates a new instance of <see cref="DefaultLoadBalancingPolicy"/> wrapping the provided child policy.
        /// </summary>
        internal DefaultLoadBalancingPolicy(ILoadBalancingPolicy childPolicy)
        {
            ChildPolicy = childPolicy ?? throw new ArgumentNullException(nameof(childPolicy));
        }
        
        /// <summary>
        ///  Creates a new datacenter aware round robin policy given the name of the local
        ///  datacenter. <p> The name of the local datacenter provided must be the local
        ///  datacenter name as known by the server. </p><p> The policy created will ignore all
        ///  remote hosts.</p>
        /// </summary>
        /// <param name="localDc"> the name of the local datacenter (as known by Cassandra).</param>
        public DefaultLoadBalancingPolicy(string localDc)
        {
            ChildPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc));
        }

        internal ILoadBalancingPolicy ChildPolicy { get; }
        
        /// <summary>
        ///  Return the HostDistance for the provided host. This policy consider nodes
        ///  in the local datacenter as <c>Local</c> and the rest
        ///  is <c>Ignored</c>.
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <c>host</c>.</returns>
        public HostDistance Distance(Host host)
        {
            var lastPreferredHost = _lastPreferredHost;
            if (lastPreferredHost != null && host == lastPreferredHost)
            {
                // Set the last preferred host as local.
                // It's somewhat "hacky" but ensures that the pool for the graph analytics host has the appropriate size
                return HostDistance.Local;
            }

            return ChildPolicy.Distance(host);
        }

        /// <summary>
        /// Initializes the policy.
        /// </summary>
        public void Initialize(ICluster cluster)
        {
            ChildPolicy.Initialize(cluster);
        }

        /// <summary>
        /// Returns the hosts to used for a query.
        /// </summary>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement statement)
        {
            if (statement is TargettedSimpleStatement targetedStatement && targetedStatement.PreferredHost != null)
            {
                _lastPreferredHost = targetedStatement.PreferredHost;
                return YieldPreferred(keyspace, targetedStatement);
            }

            return ChildPolicy.NewQueryPlan(keyspace, statement);
        }

        private IEnumerable<Host> YieldPreferred(string keyspace, TargettedSimpleStatement statement)
        {
            yield return statement.PreferredHost;
            foreach (var h in ChildPolicy.NewQueryPlan(keyspace, statement))
            {
                yield return h;
            }
        }
    }
}
