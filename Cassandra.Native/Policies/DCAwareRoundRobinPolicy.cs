using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
{

    /**
     * A data-center aware Round-robin load balancing policy.
     * <p>
     * This policy provides round-robin queries over the node of the local
     * datacenter. It also includes in the query plans returned a configurable
     * number of hosts in the remote datacenters, but those are always tried
     * after the local nodes. In other words, this policy guarantees that no
     * host in a remote datacenter will be queried unless no host in the local
     * datacenter can be reached.
     * <p>
     * If used with a single datacenter, this policy is equivalent to the
     * {@code LoadBalancingPolicy.RoundRobin} policy, but its DC awareness
     * incurs a slight overhead so the {@code LoadBalancingPolicy.RoundRobin}
     * policy could be prefered to this policy in that case.
     */
    public class DCAwareRoundRobinPolicy : LoadBalancingPolicy
    {

        private readonly string localDc;
        private readonly int usedHostsPerRemoteDc;
        ICassandraSessionInfoProvider infoProvider;

        /**
         * Creates a new datacenter aware round robin policy given the name of
         * the local datacenter.
         * <p>
         * The name of the local datacenter provided must be the local
         * datacenter name as known by Cassandra.
         * <p>
         * The policy created will ignore all remote hosts. In other words,
         * this is equivalent to {@code new DCAwareRoundRobinPolicy(localDc, 0)}.
         *
         * @param localDc the name of the local datacenter (as known by
         * Cassandra).
         */
        public DCAwareRoundRobinPolicy(string localDc)
            : this(localDc, 0)
        {
        }

        /**
         * Creates a new DCAwareRoundRobin policy given the name of the local
         * datacenter and that uses the provided number of host per remote
         * datacenter as failover for the local hosts.
         * <p>
         * The name of the local datacenter provided must be the local
         * datacenter name as known by Cassandra.
         *
         * @param localDc the name of the local datacenter (as known by
         * Cassandra).
         * @param usedHostsPerRemoteDc the number of host per remote
         * datacenter that policies created by the returned factory should
         * consider. Created policies {@code distance} method will return a
         * {@code HostDistance.REMOTE} distance for only {@code
         * usedHostsPerRemoteDc} hosts per remote datacenter. Other hosts
         * of the remote datacenters will be ignored (and thus no
         * connections to them will be maintained).
         */
        public DCAwareRoundRobinPolicy(string localDc, int usedHostsPerRemoteDc)
        {
            this.localDc = localDc;
            this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
        }


        public void Initialize(ICassandraSessionInfoProvider infoProvider)
        {
            this.infoProvider = infoProvider;
        }

        private string DC(CassandraClusterHost host)
        {
            string dc = host.Datacenter;
            return dc == null ? localDc : dc;
        }

        public CassandraHostDistance Distance(CassandraClusterHost host)
        {
            string dc = DC(host);
            if (dc.Equals(localDc))
                return CassandraHostDistance.LOCAL;

            int ix = 0;
            foreach (var h in infoProvider.GetAllHosts())
            {
                if (h == host)
                    return ix < usedHostsPerRemoteDc ? CassandraHostDistance.IGNORED : CassandraHostDistance.REMOTE;
                else if (dc.Equals(DC(h)))
                    ix++;
            }
            return CassandraHostDistance.IGNORED;
        }

        public IEnumerable<CassandraClusterHost> NewQueryPlan(CassandraRoutingKey routingKey)
        {
            foreach (var h in infoProvider.GetAllHosts())
            {
                if (localDc.Equals(DC(h)))
                {
                    if (h.IsUp)
                        yield return h;
                }
            }
            Dictionary<string, int> ixes = new Dictionary<string, int>();
            foreach (var h in infoProvider.GetAllHosts())
            {
                if (!localDc.Equals(DC(h)))
                {
                    if (h.IsUp && (!ixes.ContainsKey(DC(h)) || ixes[DC(h)] < usedHostsPerRemoteDc))
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
