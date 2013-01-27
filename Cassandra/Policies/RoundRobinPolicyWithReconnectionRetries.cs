using System;
using System.Collections.Generic;
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
    public class RoundRobinPolicyWithReconnectionRetries : ILoadBalancingPolicy
    {

        public RoundRobinPolicyWithReconnectionRetries(IReconnectionPolicy reconnectionPolicy)
        {
            this._reconnectionPolicy = reconnectionPolicy;
        }


        public EventHandler<RoundRobinPolicyWithReconnectionRetriesEventArgs> ReconnectionEvent;

        readonly IReconnectionPolicy _reconnectionPolicy;
        Cluster _cluster;
        int _startidx = -1;

        public void Initialize(Cluster cluster)
        {
            this._cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
        }

        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            var schedule = _reconnectionPolicy.NewSchedule();
            while (true)
            {
                List<Host> copyOfHosts = new List<Host>(_cluster.Metadata.AllHosts());
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
