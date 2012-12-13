using System;
using System.Collections.Generic;
using System.Text;

/**
 * A Round-robin load balancing policy.
 * <p>
 * This policy queries nodes in a round-robin fashion. For a given query,
 * if an host fail, the next one (following the round-robin order) is
 * tried, until all hosts have been tried.
 * <p>
 * This policy is not datacenter aware and will include every known
 * Cassandra host in its round robin algorithm. If you use multiple
 * datacenter this will be inefficient and you will want to use the
 * {@link DCAwareRoundRobinPolicy} load balancing policy instead.
 */
namespace Cassandra.Native.Policies
{
    public class RoundRobinPolicy : LoadBalancingPolicy
    {
        /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
        public RoundRobinPolicy() { }

        ICassandraSessionInfoProvider infoProvider;
        int startidx = -1;

        public void init(ICassandraSessionInfoProvider infoProvider)
        {
            this.infoProvider = infoProvider;
        }

        /**
         * Return the HostDistance for the provided host.
         * <p>
         * This policy consider all nodes as local. This is generally the right
         * thing to do in a single datacenter deployement. If you use multiple
         * datacenter, see {@link DCAwareRoundRobinPolicy} instead.
         *
         * @param host the host of which to return the distance of.
         * @return the HostDistance to {@code host}.
         */
        public CassandraHostDistance distance(CassandraClusterHost host)
        {
            return CassandraHostDistance.LOCAL;
        }

        /**
         * Returns the hosts to use for a new query.
         * <p>
         * The returned plan will try each known host of the cluster. Upon each
         * call to this method, the ith host of the plans returned will cycle
         * over all the host of the cluster in a round-robin fashion.
         *
         * @param query the query for which to build the plan.
         * @return a new query plan, i.e. an iterator indicating which host to
         * try first for querying, which one to use as failover, etc...
         */
        public IEnumerable<CassandraClusterHost> newQueryPlan(CassandraRoutingKey routingKey)
        {
            List<CassandraClusterHost> copyOfHosts = new List<CassandraClusterHost>(infoProvider.GetAllHosts());
            if (startidx == -1 || startidx >= copyOfHosts.Count - 1)
                startidx = StaticRandom.Instance.Next(copyOfHosts.Count - 1);
            for (int i = 0; i < copyOfHosts.Count; i++)
            {
                var h = copyOfHosts[startidx++];
                if (h.isUp)
                    yield return h;
            }
        }
    }
}
