using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.Policies.Util
{
    public class CustomLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private ICluster _cluster;
        private readonly string[] _hosts;

        public CustomLoadBalancingPolicy(string[] hosts)
        {
            _hosts = hosts;
        }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            var queryPlan = new List<Host>();
            var allHosts = _cluster.AllHosts();
            foreach (var host in _hosts)
            {
                queryPlan.Add(allHosts.Single(h => h.Address.ToString() == host));
            }
            return queryPlan;
        }
    }
}
