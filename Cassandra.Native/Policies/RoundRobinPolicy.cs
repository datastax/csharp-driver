using System;
using System.Collections.Generic;
using System.Text;
using Cassandra;

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
namespace Cassandra
{
    public class RoundRobinPolicy : LoadBalancingPolicy
    {
        /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
        public RoundRobinPolicy() { }

        ISessionInfoProvider _infoProvider;
        int _startidx = -1;

        public void Initialize(ISessionInfoProvider infoProvider)
        {
            this._infoProvider = infoProvider;
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
        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
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
        public IEnumerable<Host> NewQueryPlan(CassandraRoutingKey routingKey)
        {
            List<Host> copyOfHosts = new List<Host>(_infoProvider.GetAllHosts());
            for (int i = 0; i < copyOfHosts.Count; i++)
            {
                if (_startidx == -1 || _startidx >= copyOfHosts.Count - 1)
                    _startidx = StaticRandom.Instance.Next(copyOfHosts.Count - 1);

                var h = copyOfHosts[_startidx];
                if (h.IsConsiderablyUp)
                    yield return h;

                _startidx++;
                _startidx = _startidx % copyOfHosts.Count;
            }
        }
    }
}
