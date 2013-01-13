using System.Collections.Generic;

namespace Cassandra
{
    /**
     * A wrapper load balancing policy that add token awareness to a child policy.
     * <p>
     * This policy encapsulates another policy. The resulting policy works in
     * the following way:
     * <ul>
     *   <li>the {@code distance} method is inherited from the child policy.</li>
     *   <li>the iterator return by the {@code newQueryPlan} method will first
     *   return the {@code LOCAL} replicas for the query (based on {@link Query#getRoutingKey})
     *   <i>if possible</i> (i.e. if the query {@code getRoutingKey} method
     *   doesn't return {@code null} and if {@link Metadata#getReplicas}
     *   returns a non empty set of replicas for that partition key). If no
     *   local replica can be either found or successfully contacted, the rest
     *   of the query plan will fallback to one of the child policy.</li>
     * </ul>
     * <p>
     * Do note that only replica for which the child policy {@code distance}
     * method returns {@code HostDistance.LOCAL} will be considered having
     * priority. For example, if you wrap {@link DCAwareRoundRobinPolicy} with this
     * token aware policy, replicas from remote data centers may only be
     * returned after all the host of the local data center.
     */
    public class TokenAwarePolicy : LoadBalancingPolicy
    {

        private ISessionInfoProvider _infoProvider;
        private readonly LoadBalancingPolicy _childPolicy;

        /**
         * Creates a new {@code TokenAware} policy that wraps the provided child
         * load balancing policy.
         *
         * @param childPolicy the load balancing policy to wrap with token
         * awareness.
         */
        public TokenAwarePolicy(LoadBalancingPolicy childPolicy)
        {
            this._childPolicy = childPolicy;
        }


        public void Initialize(ISessionInfoProvider infoProvider)
        {
            this._infoProvider = infoProvider;
            _childPolicy.Initialize(infoProvider);
        }

        public HostDistance Distance(Host host)
        {
            return _childPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(CassandraRoutingKey routingKey)
        {
            if (routingKey == null)
            {
                foreach (var iter in _childPolicy.NewQueryPlan(null))
                    yield return iter;
                yield break;
            }

            var replicas = _infoProvider.GetReplicas(routingKey.RawRoutingKey);
            if (replicas.Count == 0)
            {
                foreach (var iter in _childPolicy.NewQueryPlan(routingKey))
                    yield return iter;
                yield break;
            }

            var iterator = replicas.GetEnumerator();
            while (iterator.MoveNext())
            {
                var host = iterator.Current;
                if (host.IsConsiderablyUp && _childPolicy.Distance(host) == HostDistance.Local)
                    yield return host;
            }

            foreach (var host in _childPolicy.NewQueryPlan(routingKey))
            {
                if (!replicas.Contains(host) || _childPolicy.Distance(host) != HostDistance.Local)
                    yield return host;
            }

        }
    }
}