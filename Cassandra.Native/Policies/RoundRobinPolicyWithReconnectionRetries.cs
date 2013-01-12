using System;
using System.Collections.Generic;
using System.Text;
using Cassandra;
using System.Threading;

namespace Cassandra
{
    public class RoundRobinPolicyWithReconnectionRetriesEventArgs : EventArgs
    {
        public long DelayMs { get; private set; }
        public bool Cancel = false;
        public RoundRobinPolicyWithReconnectionRetriesEventArgs(long delayMs)
        {
            this.DelayMs = delayMs;
        }
    }
    public class RoundRobinPolicyWithReconnectionRetries : LoadBalancingPolicy
    {
        /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
        public RoundRobinPolicyWithReconnectionRetries(ReconnectionPolicy reconnectionPolicy)
        {
            this._reconnectionPolicy = reconnectionPolicy;
        }


        public EventHandler<RoundRobinPolicyWithReconnectionRetriesEventArgs> ReconnectionEvent;

        readonly ReconnectionPolicy _reconnectionPolicy;
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
            var schedule = _reconnectionPolicy.NewSchedule();
            while (true)
            {
                List<Host> copyOfHosts = new List<Host>(_infoProvider.GetAllHosts());
                for (int i = 0; i < copyOfHosts.Count; i++)
                {
                    if (_startidx == -1 || _startidx >= copyOfHosts.Count - 1)
                        _startidx = StaticRandom.Instance.Next(copyOfHosts.Count);

                    var h = copyOfHosts[_startidx];
                    if (h.IsConsiderablyUp)
                        yield return h;

                    _startidx++;
                    _startidx = _startidx % copyOfHosts.Count;
                }
                if (ReconnectionEvent != null)
                {
                    var ea = new RoundRobinPolicyWithReconnectionRetriesEventArgs(schedule.NextDelayMs());
                    ReconnectionEvent(this, ea);
                    if (ea.Cancel)
                        break;
                }
                else
                    Thread.Sleep((int)schedule.NextDelayMs());
            }
        }
    }
}
