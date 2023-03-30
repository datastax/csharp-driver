//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Threading;

namespace Cassandra
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
