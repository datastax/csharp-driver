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
using System.Linq;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    /// A wrapper load balancing policy that adds token awareness to a child policy.
    /// <para> This policy encapsulates another policy. The resulting policy works in the following way:
    /// </para>
    /// <list type="number">
    /// <item>The <see cref="Distance(Host)"/> method is inherited  from the child policy.</item>
    /// <item>The host yielded by the <see cref="NewQueryPlan(string, IStatement)"/> method will first return the
    /// <see cref="HostDistance.Local"/> replicas for the statement, based on the <see cref="Statement.RoutingKey"/>.
    /// </item>
    /// </list>
    /// </summary>
    public class TokenAwarePolicy : ILoadBalancingPolicy
    {
        private ICluster _cluster;
        private readonly ThreadLocal<Random> _prng = new ThreadLocal<Random>(() => new Random(
            // Predictable random numbers are OK
            Environment.TickCount * Environment.CurrentManagedThreadId));

        /// <summary>
        ///  Creates a new <c>TokenAware</c> policy that wraps the provided child
        ///  load balancing policy.
        /// </summary>
        /// <param name="childPolicy"> the load balancing policy to wrap with token
        ///  awareness.</param>
        public TokenAwarePolicy(ILoadBalancingPolicy childPolicy)
        {
            ChildPolicy = childPolicy;
        }

        public ILoadBalancingPolicy ChildPolicy { get; }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;
            ChildPolicy.Initialize(cluster);
        }

        /// <summary>
        ///  Return the HostDistance for the provided host.
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// 
        /// <returns>the HostDistance to <c>host</c> as returned by the wrapped
        ///  policy.</returns>
        public HostDistance Distance(Host host)
        {
            return ChildPolicy.Distance(host);
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will first
        ///  return replicas (whose <c>HostDistance</c> for the child policy is
        ///  <c>Local</c>) for the query if it can determine them (i.e. mainly if
        ///  <c>IStatement.RoutingKey</c> is not <c>null</c>). Following what
        ///  it will return the plan of the child policy.</p>
        /// </summary>
        /// <param name="loggedKeyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>the new query plan.</returns>
        public IEnumerable<Host> NewQueryPlan(string loggedKeyspace, IStatement query)
        {
            var routingKey = query?.RoutingKey;
            IEnumerable<Host> childIterator;
            if (routingKey == null)
            {
                childIterator = ChildPolicy.NewQueryPlan(loggedKeyspace, query);
                foreach (var h in childIterator)
                {
                    yield return h;
                }
                yield break;
            }

            var keyspace = query.Keyspace ?? loggedKeyspace;
            var replicas = _cluster.GetReplicas(keyspace, routingKey.RawRoutingKey);

            var localReplicaSet = new HashSet<Host>();
            var localReplicaList = new List<Host>(replicas.Count);
            // We can't do it lazily as we need to balance the load between local replicas
            foreach (var localReplica in replicas.Where(h => ChildPolicy.Distance(h) == HostDistance.Local))
            {
                localReplicaSet.Add(localReplica);
                localReplicaList.Add(localReplica);
            }
            // Return the local replicas first
            if (localReplicaList.Count > 0)
            {
                // Use a pseudo random start index
                var startIndex = _prng.Value.Next();
                for (var i = 0; i < localReplicaList.Count; i++)
                {
                    yield return localReplicaList[(startIndex + i) % localReplicaList.Count];
                }
            }

            // Then, return the rest of child policy hosts
            childIterator = ChildPolicy.NewQueryPlan(loggedKeyspace, query);
            foreach (var h in childIterator)
            {
                if (localReplicaSet.Contains(h))
                {
                    continue;
                }
                yield return h;
            }
        }
    }
}
