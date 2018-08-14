using System.Collections.Generic;
using System.Linq;

namespace Cassandra
{
    public class BlackListedDCLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private ILoadBalancingPolicy _childPolicy;

        private readonly IList<string> blacklistedDC;

        public BlackListedDCLoadBalancingPolicy(IList<string> blacklistedDC)
            : this(blacklistedDC, Policies.DefaultLoadBalancingPolicy)
        {
        }

        public BlackListedDCLoadBalancingPolicy(IList<string> blacklistedDC, ILoadBalancingPolicy childPolicy)
        {
            _childPolicy = childPolicy;
            this.blacklistedDC = blacklistedDC;
        }

        public HostDistance Distance(Host host)
        {
            if (blacklistedDC?.Any(x => x?.ToLower() == host.Datacenter.ToLower()) ?? false)
                return HostDistance.Ignored;
            return _childPolicy.Distance(host); ;
        }

        public void Initialize(ICluster cluster)
        {
            _childPolicy.Initialize(cluster);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            return _childPolicy.NewQueryPlan(keyspace, query);
        }
    }
}
