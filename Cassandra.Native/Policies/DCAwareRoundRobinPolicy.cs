using System.Collections.Generic;

namespace Cassandra
{

    ///<summary>
    /// A data-center aware Round-robin load balancing policy.
    /// <p>
    /// This policy provides round-robin queries over the node of the local
    /// datacenter. It also includes in the query plans returned a configurable
    /// number of hosts in the remote datacenters, but those are always tried
    /// after the local nodes. In other words, this policy guarantees that no
    /// host in a remote datacenter will be queried unless no host in the local
    /// datacenter can be reached.</p>
    /// <p>
    /// If used with a single datacenter, this policy is equivalent to the
    /// {@code LoadBalancingPolicy.RoundRobin} policy, but its DC awareness
    /// incurs a slight overhead so the {@code LoadBalancingPolicy.RoundRobin}
    /// policy could be prefered to this policy in that case.</p>
    ///</summary>
    public class DCAwareRoundRobinPolicy : LoadBalancingPolicy
    {

        private readonly string _localDc;
        private readonly int _usedHostsPerRemoteDc;
        ISessionInfoProvider _infoProvider;

        ///<summary>
        /// Creates a new datacenter aware round robin policy given the name of
        /// the local datacenter.
        /// <p>
        /// The name of the local datacenter provided must be the local
        /// datacenter name as known by Cassandra.</p>
        /// <p>
        /// The policy created will ignore all remote hosts. In other words,
        /// this is equivalent to {@code new DCAwareRoundRobinPolicy(localDc, 0)}.</p>
        ///
        /// <param name="localDc">the name of the local datacenter (as known by
        /// Cassandra).</param>
        ///</summary>
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
        /// <code>HostDistance.REMOTE</code> distance for only <code>
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

        public IEnumerable<Host> NewQueryPlan(CassandraRoutingKey routingKey)
        {
            foreach (var h in _infoProvider.GetAllHosts())
            {
                if (_localDc.Equals(DC(h)))
                {
                    if (h.IsConsiderablyUp)
                        yield return h;
                }
            }
            Dictionary<string, int> ixes = new Dictionary<string, int>();
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
