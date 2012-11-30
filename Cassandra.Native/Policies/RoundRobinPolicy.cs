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



        public void init(CassandraCluster cluster, IEnumerable<CassandraClusterHost> hosts)
        {
            throw new NotImplementedException();
        }

        public CassandraHostDistance distance(CassandraClusterHost host)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<CassandraClusterHost> newQueryPlan()
        {
            throw new NotImplementedException();
        }
    }
}
