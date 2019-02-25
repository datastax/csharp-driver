//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Threading;

namespace Dse
{
    public class RetryLoadBalancingPolicy : ILoadBalancingPolicy
    {
        public EventHandler<RetryLoadBalancingPolicyEventArgs> ReconnectionEvent;

        public RetryLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy, IReconnectionPolicy reconnectionPolicy)
        {
            ReconnectionPolicy = reconnectionPolicy;
            LoadBalancingPolicy = loadBalancingPolicy;
        }

        public IReconnectionPolicy ReconnectionPolicy { get; }

        public ILoadBalancingPolicy LoadBalancingPolicy { get; }

        public void Initialize(ICluster cluster)
        {
            LoadBalancingPolicy.Initialize(cluster);
        }

        public HostDistance Distance(Host host)
        {
            return LoadBalancingPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            IReconnectionSchedule schedule = ReconnectionPolicy.NewSchedule();
            while (true)
            {
                IEnumerable<Host> childQueryPlan = LoadBalancingPolicy.NewQueryPlan(keyspace, query);
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
