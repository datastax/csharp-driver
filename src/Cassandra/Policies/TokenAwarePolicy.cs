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

using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  A wrapper load balancing policy that add token awareness to a child policy.
    ///  <p> This policy encapsulates another policy. The resulting policy works in
    ///  the following way: <ul> <li>the <c>distance</c> method is inherited
    ///  from the child policy.</li> <li>the iterator return by the
    ///  <c>newQueryPlan</c> method will first return the <c>LOCAL</c>
    ///  replicas for the query (based on <link>Query#getRoutingKey</link>) <i>if
    ///  possible</i> (i.e. if the query <c>getRoutingKey</c> method doesn't
    ///  return {@code null} and if {@link Metadata#getReplicas}' returns a non empty
    ///  set of replicas for that partition key). If no local replica can be either
    ///  found or successfully contacted, the rest of the query plan will fallback to
    ///  one of the child policy.</li> </ul> </p><p> Do note that only replica for which
    ///  the child policy <c>distance</c> method returns
    ///  <c>HostDistance.Local</c> will be considered having priority. For
    ///  example, if you wrap <link>DCAwareRoundRobinPolicy</link> with this token
    ///  aware policy, replicas from remote data centers may only be returned after
    ///  all the host of the local data center.</p>
    /// </summary>
    public class TokenAwarePolicy : ILoadBalancingPolicy
    {
        private readonly ILoadBalancingPolicy _childPolicy;
        private ICluster _cluster;
        int _index;

        /// <summary>
        ///  Creates a new <c>TokenAware</c> policy that wraps the provided child
        ///  load balancing policy.
        /// </summary>
        /// <param name="childPolicy"> the load balancing policy to wrap with token
        ///  awareness.</param>
        public TokenAwarePolicy(ILoadBalancingPolicy childPolicy)
        {
            _childPolicy = childPolicy;
        }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;
            _childPolicy.Initialize(cluster);
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
            return _childPolicy.Distance(host);
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will first
        ///  return replicas (whose <c>HostDistance</c> for the child policy is
        ///  <c>Local</c>) for the query if it can determine them (i.e. mainly if
        ///  <c>IStatement.RoutingKey</c> is not <c>null</c>). Following what
        ///  it will return the plan of the child policy.</p>
        /// </summary>
        /// <param name="keyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>the new query plan.</returns>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            var routingKey = query == null ? null : query.RoutingKey;
            IEnumerable<Host> childIterator;
            if (routingKey == null)
            {
                childIterator = _childPolicy.NewQueryPlan(keyspace, query);
                foreach (var h in childIterator)
                {
                    yield return h;
                }
                yield break;
            }
            var replicas = _cluster.GetReplicas(keyspace, routingKey.RawRoutingKey);
            //We need to have split into local and remote replicas
            //We need actual lists (not lazy) as we need to round-robin through them
            var localReplicas = new List<Host>();
            var remoteReplicas = new List<Host>();
            foreach (var h in replicas)
            {
                var distance = _childPolicy.Distance(h);
                if (distance == HostDistance.Local)
                {
                    localReplicas.Add(h);
                }
                else if (distance == HostDistance.Remote)
                {
                    remoteReplicas.Add(h);
                }
            }
            //Return the local replicas first
            if (localReplicas.Count > 0)
            {
                //Round robin through the local replicas
                var roundRobinIndex = Interlocked.Increment(ref _index);
                //Overflow protection
                if (roundRobinIndex > int.MaxValue - 10000)
                {
                    Interlocked.Exchange(ref _index, 0);
                }
                for (var i = 0; i < localReplicas.Count; i++)
                {
                    yield return localReplicas[(roundRobinIndex + i)%localReplicas.Count];
                }
            }

            //Then, return the child policy hosts
            childIterator = _childPolicy.NewQueryPlan(keyspace, query);
            foreach (var h in childIterator)
            {
                //It is yielded with the rest of replicas
                if (replicas.Contains(h))
                {
                    continue;
                }
                yield return h;
            }

            //Then, the remote replicas
            foreach (var h in remoteReplicas)
            {
                yield return h;
            }
        }
    }
}
