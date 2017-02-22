//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Threading;

namespace Cassandra
{
    public class RetryLoadBalancingPolicy : ILoadBalancingPolicy
    {
        private readonly ILoadBalancingPolicy _loadBalancingPolicy;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        public EventHandler<RetryLoadBalancingPolicyEventArgs> ReconnectionEvent;

        public RetryLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy, IReconnectionPolicy reconnectionPolicy)
        {
            _reconnectionPolicy = reconnectionPolicy;
            _loadBalancingPolicy = loadBalancingPolicy;
        }

        public void Initialize(ICluster cluster)
        {
            _loadBalancingPolicy.Initialize(cluster);
        }

        public HostDistance Distance(Host host)
        {
            return _loadBalancingPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            IReconnectionSchedule schedule = _reconnectionPolicy.NewSchedule();
            while (true)
            {
                IEnumerable<Host> childQueryPlan = _loadBalancingPolicy.NewQueryPlan(keyspace, query);
                foreach (Host host in childQueryPlan)
                    yield return host;

                if (ReconnectionEvent != null)
                {
                    var ea = new RetryLoadBalancingPolicyEventArgs(schedule.NextDelayMs());
                    ReconnectionEvent(this, ea);
                    if (ea.Cancel)
                        break;
                }
                else
                    Thread.Sleep((int) schedule.NextDelayMs());
            }
        }
    }
}
