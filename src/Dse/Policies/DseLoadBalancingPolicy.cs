//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

namespace Dse
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
        private volatile Host _lastPreferredHost;

        /// <summary>
        /// Creates a new instance of <see cref="DseLoadBalancingPolicy"/> wrapping the provided child policy.
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
        /// Creates a new instance of <see cref="DseLoadBalancingPolicy"/> given the name of the local datacenter and
        /// the amount of host per remote datacenter to use for failover for the local hosts.
        /// </summary>
        /// <param name="localDc">The name of the local datacenter (case-sensitive)</param>
        /// <param name="usedHostsPerRemoteDc">
        /// The amount of host per remote datacenter that the policy should yield in a new query plan after the local
        /// nodes.
        /// </param>
        public DseLoadBalancingPolicy(string localDc, int usedHostsPerRemoteDc = 0)
        {
            _childPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc));
        }

        /// <summary>
        /// Creates the default load balancing policy, using 
        /// <see cref="Policies.DefaultLoadBalancingPolicy"/> as child policy.
        /// </summary>
        public static DseLoadBalancingPolicy CreateDefault()
        {
            return new DseLoadBalancingPolicy(Policies.DefaultLoadBalancingPolicy);
        }

        /// <summary>
        /// Returns the distance as determined by the child policy.
        /// </summary>
        public HostDistance Distance(Host host)
        {
            var lastPreferredHost = _lastPreferredHost;
            if (lastPreferredHost != null && host == lastPreferredHost)
            {
                // Set the last preferred host as local.
                // It's somewhat "hacky" but ensures that the pool for the graph analytics host has the appropriate size
                return HostDistance.Local;
            }
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
                _lastPreferredHost = targettedStatement.PreferredHost;
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
