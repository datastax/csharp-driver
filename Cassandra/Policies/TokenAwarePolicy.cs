//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  A wrapper load balancing policy that add token awareness to a child policy.
    ///  <p> This policy encapsulates another policy. The resulting policy works in
    ///  the following way: <ul> <li>the <code>distance</code> method is inherited
    ///  from the child policy.</li> <li>the iterator return by the
    ///  <code>newQueryPlan</code> method will first return the <code>LOCAL</code>
    ///  replicas for the query (based on <link>Query#getRoutingKey</link>) <i>if
    ///  possible</i> (i.e. if the query <code>getRoutingKey</code> method doesn't
    ///  return {@code null} and if {@link Metadata#getReplicas}' returns a non empty
    ///  set of replicas for that partition key). If no local replica can be either
    ///  found or successfully contacted, the rest of the query plan will fallback to
    ///  one of the child policy.</li> </ul> </p><p> Do note that only replica for which
    ///  the child policy <code>distance</code> method returns
    ///  <code>HostDistance.Local</code> will be considered having priority. For
    ///  example, if you wrap <link>DCAwareRoundRobinPolicy</link> with this token
    ///  aware policy, replicas from remote data centers may only be returned after
    ///  all the host of the local data center.</p>
    /// </summary>
    public class TokenAwarePolicy : ILoadBalancingPolicy
    {

        private Cluster _cluster;
        private readonly ILoadBalancingPolicy _childPolicy;

        /// <summary>
        ///  Creates a new <code>TokenAware</code> policy that wraps the provided child
        ///  load balancing policy.
        /// </summary>
        /// <param name="childPolicy"> the load balancing policy to wrap with token
        ///  awareness.</param>
        public TokenAwarePolicy(ILoadBalancingPolicy childPolicy)
        {
            this._childPolicy = childPolicy;
        }

        public void Initialize(Cluster cluster)
        {
            this._cluster = cluster;
            _childPolicy.Initialize(cluster);
        }


        /// <summary>
        ///  Return the HostDistance for the provided host.
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// 
        /// <returns>the HostDistance to <code>host</code> as returned by the wrapped
        ///  policy.</returns>
        public HostDistance Distance(Host host)
        {
            return _childPolicy.Distance(host);
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will first
        ///  return replicas (whose <code>HostDistance</code> for the child policy is
        ///  <code>Local</code>) for the query if it can determine them (i.e. mainly if
        ///  <code>query.getRoutingKey()</code> is not <code>null</code>). Following what
        ///  it will return the plan of the child policy.</p>
        /// </summary>
        /// <param name="query"> the query for which to build the plan. </param>
        /// 
        /// <returns>the new query plan.</returns>
        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            var routingKey = query == null ? null : query.RoutingKey;
            if (routingKey == null)
            {
                foreach (var iter in _childPolicy.NewQueryPlan(null))
                    yield return iter;
                yield break;
            }

            var replicas = _cluster.Metadata.GetReplicas(routingKey.RawRoutingKey);
            if (replicas.Count == 0)
            {
                foreach (var iter in _childPolicy.NewQueryPlan(query))
                    yield return iter;
                yield break;
            }

            var iterator = replicas.GetEnumerator();
            while (iterator.MoveNext())
            {
                var host = _cluster.Metadata.GetHost(iterator.Current);
                if (host != null && host.IsConsiderablyUp && _childPolicy.Distance(host) == HostDistance.Local)
                    yield return host;
            }

            var childIterator = _childPolicy.NewQueryPlan(query);
            var remoteChildren = new HashSet<Host>();
            foreach (var host in childIterator)
            {
                if (!replicas.Contains(host.Address))
                {
                    if (_childPolicy.Distance(host) != HostDistance.Local)
                        remoteChildren.Add(host);
                    else
                        yield return host;
                }
            }

            foreach (var host in remoteChildren)
            {
                yield return host;
            }

        }
    }
}