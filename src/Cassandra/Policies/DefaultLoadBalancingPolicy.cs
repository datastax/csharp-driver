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
    ///  For most workloads, the query plan will be determined by the child load balancing policy (by default, TokenAwarePolicy(DCAwareRoundRobinPolicy)).
    /// </para>
    /// <para>
    ///  For graph analytics queries, this policy sets the preferred analytics graph server
    ///  previously obtained by driver as the first host in the query plan. After this host, the query plan is the same as the one
    /// returned by the child policy.
    /// </para>
    /// </summary>
    public class DefaultLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private const string UsedHostsPerRemoteDcObsoleteMessage =
            "The usedHostsPerRemoteDc parameter will be removed in the next major release of the driver. " +
            "DC failover should not be done in the driver, which does not have the necessary context to know " +
            "what makes sense considering application semantics. See https://datastax-oss.atlassian.net/browse/CSHARP-722";

        private volatile Host _lastPreferredHost;

        /// <summary>
        /// Creates a new instance of <see cref="DefaultLoadBalancingPolicy"/> wrapping the provided child policy.
        /// </summary>
        public DefaultLoadBalancingPolicy(ILoadBalancingPolicy childPolicy)
        {
            ChildPolicy = childPolicy ?? throw new ArgumentNullException(nameof(childPolicy));
        }

        /// <summary>
        /// Creates a new instance of <see cref="DefaultLoadBalancingPolicy"/> given the name of the local datacenter and
        /// the amount of host per remote datacenter to use for failover for the local hosts.
        /// </summary>
        /// <param name="localDc">The name of the local datacenter (case-sensitive)</param>
        /// <param name="usedHostsPerRemoteDc">
        /// The amount of host per remote datacenter that the policy should yield in a new query plan after the local
        /// nodes. Note that this parameter will be removed in the next major version of the driver.
        /// </param>
        [Obsolete(DefaultLoadBalancingPolicy.UsedHostsPerRemoteDcObsoleteMessage)]
        public DefaultLoadBalancingPolicy(string localDc, int usedHostsPerRemoteDc)
        {
#pragma warning disable 618
            ChildPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc));
#pragma warning restore 618
        }

        /// <summary>
        /// Creates a new instance of <see cref="DefaultLoadBalancingPolicy"/> given the name of the local datacenter.
        /// </summary>
        /// <param name="localDc">The name of the local datacenter (case-sensitive)</param>
        public DefaultLoadBalancingPolicy(string localDc)
        {
            ChildPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc));
        }

        internal ILoadBalancingPolicy ChildPolicy { get; }
        
        /// <summary>
        /// Returns the distance as determined by the child policy.
        /// </summary>
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
