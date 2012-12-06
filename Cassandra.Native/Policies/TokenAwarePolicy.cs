using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
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

        /**
         * Creates a new {@code TokenAware} policy that wraps the provided child
         * load balancing policy.
         *
         * @param childPolicy the load balancing policy to wrap with token
         * awareness.
         */
        public TokenAwarePolicy(LoadBalancingPolicy childPolicy)
        {
        }


        public void init(ICollection<CassandraClusterHost> hosts)
        {
            throw new NotImplementedException();
        }

        public CassandraHostDistance distance(CassandraClusterHost host)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<CassandraClusterHost> newQueryPlan(CassandraRoutingKey routingKey)
        {
            throw new NotImplementedException();
        }
    }
}