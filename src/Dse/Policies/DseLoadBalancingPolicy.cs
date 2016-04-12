using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Policies
{
    /// <summary>
    /// A load balancing policy designed to run against DSE cluster.
    /// <para>
    ///  For most executions, the query plan will be determined by the child load balancing policy.
    ///  Except for some cases, like graph analytics queries, for which it uses the preferred analytics graph server
    ///  previously obtained by driver as first host in the query plan.
    /// </para>
    /// </summary>
    public class DseLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private readonly ILoadBalancingPolicy _childPolicy;

        /// <summary>
        /// Creates a new instance of <see cref="DseLoadBalancingPolicy"/>.
        /// </summary>
        public DseLoadBalancingPolicy(ILoadBalancingPolicy childPolicy)
        {
            if (childPolicy == null)
            {
                throw new ArgumentNullException("childPolicy");
            }
            _childPolicy = childPolicy;
        }

        /// <summary>
        /// Creates the default load balancing policy, using 
        /// <see cref="Cassandra.Policies.DefaultLoadBalancingPolicy"/> as child policy.
        /// </summary>
        public static DseLoadBalancingPolicy CreateDefault()
        {
            return new DseLoadBalancingPolicy(Cassandra.Policies.DefaultLoadBalancingPolicy);
        }

        /// <summary>
        /// Returns the distance as determined by the child policy.
        /// </summary>
        public HostDistance Distance(Host host)
        {
            return _childPolicy.Distance(host);
        }

        /// <summary>
        /// Initializes the policy.
        /// </summary>
        public void Initialize(ICluster cluster)
        {
            _childPolicy.Initialize(cluster);
        }

        /// <summary>
        /// Returns the hosts to used for a query.
        /// </summary>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement statement)
        {
            var targettedStatement = statement as TargettedSimpleStatement;
            if (targettedStatement != null && targettedStatement.PreferredHost != null)
            {
                return YieldPreferred(keyspace, targettedStatement);
            }
            return _childPolicy.NewQueryPlan(keyspace, statement);
        }

        private IEnumerable<Host> YieldPreferred(string keyspace, TargettedSimpleStatement statement)
        {
            yield return statement.PreferredHost;
            foreach (var h in _childPolicy.NewQueryPlan(keyspace, statement))
            {
                yield return h;
            }
        }
    }
}
