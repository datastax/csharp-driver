using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    ///  A data-center aware Round-robin load balancing policy. <p> This policy
    ///  provides round-robin queries over the node of the local datacenter. It also
    ///  includes in the query plans returned a configurable number of hosts in the
    ///  remote datacenters, but those are always tried after the local nodes. In
    ///  other words, this policy guarantees that no host in a remote datacenter will
    ///  be queried unless no host in the local datacenter can be reached. <p> If used
    ///  with a single datacenter, this policy is equivalent to the
    ///  <code>LoadBalancingPolicy.RoundRobin</code> policy, but its DC awareness
    ///  incurs a slight overhead so the <code>LoadBalancingPolicy.RoundRobin</code>
    ///  policy could be prefered to this policy in that case.
    /// </summary>
    public class DCAwareRoundRobinPolicy : ILoadBalancingPolicy
    {

        private readonly string _localDc;
        private readonly int _usedHostsPerRemoteDc;
        ISessionInfoProvider _infoProvider;

    	/// <summary>
		///  Creates a new datacenter aware round robin policy given the name of the local
		///  datacenter. <p> The name of the local datacenter provided must be the local
		///  datacenter name as known by Cassandra. <p> The policy created will ignore all
		///  remote hosts. In other words, this is equivalent to 
		///  <code>new DCAwareRoundRobinPolicy(localDc, 0)</code>.
		/// </summary>
		/// <param name="localDc"> the name of the local datacenter (as known by Cassandra).</param>
        public DCAwareRoundRobinPolicy(string localDc)
            : this(localDc, 0)
        {
        }

        ///<summary>
        /// Creates a new DCAwareRoundRobin policy given the name of the local
        /// datacenter and that uses the provided number of host per remote
        /// datacenter as failover for the local hosts.
        /// <p>
        /// The name of the local datacenter provided must be the local
        /// datacenter name as known by Cassandra.</p>
        ///
        /// <param name="localDc"> the name of the local datacenter (as known by
        /// Cassandra).</param>
        /// <param name="usedHostsPerRemoteDc"> the number of host per remote
        /// datacenter that policies created by the returned factory should
        /// consider. Created policies <code>distance</code> method will return a
        /// <code>HostDistance.Remote</code> distance for only <code>
        /// usedHostsPerRemoteDc</code> hosts per remote datacenter. Other hosts
        /// of the remote datacenters will be ignored (and thus no
        /// connections to them will be maintained).</param>
        ///</summary>
        public DCAwareRoundRobinPolicy(string localDc, int usedHostsPerRemoteDc)
        {
            this._localDc = localDc;
            this._usedHostsPerRemoteDc = usedHostsPerRemoteDc;
        }


        public void Initialize(ISessionInfoProvider infoProvider)
        {
            this._infoProvider = infoProvider;
        }

        private string DC(Host host)
        {
            string dc = host.Datacenter;
            return dc ?? _localDc;
        }

    	/// <summary>
		///  Return the HostDistance for the provided host. <p> This policy consider nodes
		///  in the local datacenter as <code>Local</code>. For each remote datacenter, it
		///  considers a configurable number of hosts as <code>Remote</code> and the rest
		///  is <code>Ignored</code>. <p> To configure how many host in each remote
		///  datacenter is considered <code>Remote</code>, see
		///  <link>#DCAwareRoundRobinPolicy(String, int)</link>.
		/// </summary>
		/// <param name="host"> the host of which to return the distance of. </param>
		/// <returns>the HostDistance to <code>host</code>.</returns>
		public HostDistance Distance(Host host)
        {
            string dc = DC(host);
            if (dc.Equals(_localDc))
                return HostDistance.Local;

            int ix = 0;
            foreach (var h in _infoProvider.GetAllHosts())
            {
                if (h == host)
                    return ix < _usedHostsPerRemoteDc ? HostDistance.Ignored : HostDistance.Remote;
                else if (dc.Equals(DC(h)))
                    ix++;
            }
            return HostDistance.Ignored;
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will always
        ///  try each known host in the local datacenter first, and then, if none of the
        ///  local host is reacheable, will try up to a configurable number of other host
        ///  per remote datacenter. The order of the local node in the returned query plan
        ///  will follow a Round-robin algorithm.
        /// </summary>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            foreach (var h in _infoProvider.GetAllHosts())
            {
                if (_localDc.Equals(DC(h)))
                {
                    if (h.IsConsiderablyUp)
                        yield return h;
                }
            }
            var ixes = new Dictionary<string, int>();
            foreach (var h in _infoProvider.GetAllHosts())
            {
                if (!_localDc.Equals(DC(h)))
                {
                    if (h.IsConsiderablyUp && (!ixes.ContainsKey(DC(h)) || ixes[DC(h)] < _usedHostsPerRemoteDc))
                    {
                        yield return h;
                        if (!ixes.ContainsKey(DC(h)))
                            ixes.Add(DC(h), 1);
                        else
                            ixes[DC(h)] = ixes[DC(h)] + 1;
                    }
                }
            }
        }
    }
}
