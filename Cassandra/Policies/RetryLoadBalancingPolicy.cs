//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using System.Collections.Generic;
using System.Threading;

namespace Cassandra
{
    public class RetryLoadBalancingPolicyEventArgs : EventArgs
    {
        public long DelayMs { get; private set; }
        public bool Cancel = false;
        public RetryLoadBalancingPolicyEventArgs(long delayMs)
        {
            this.DelayMs = delayMs;
        }
    }
    public class RetryLoadBalancingPolicy : ILoadBalancingPolicy
    {

        public RetryLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy, IReconnectionPolicy reconnectionPolicy)
        {
            this._reconnectionPolicy = reconnectionPolicy;
            this._loadBalancingPolicy = loadBalancingPolicy;
        }


        public EventHandler<RetryLoadBalancingPolicyEventArgs> ReconnectionEvent;

        readonly IReconnectionPolicy _reconnectionPolicy;
        readonly ILoadBalancingPolicy _loadBalancingPolicy;

        public void Initialize(Cluster cluster)
        {
            _loadBalancingPolicy.Initialize(cluster);
        }

        public HostDistance Distance(Host host)
        {
            return _loadBalancingPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            var schedule = _reconnectionPolicy.NewSchedule();
            while (true)
            {
                var childQueryPlan = _loadBalancingPolicy.NewQueryPlan(query);
                foreach (var host in childQueryPlan)
                    yield return host;

                if (ReconnectionEvent != null)
                {
                    var ea = new RetryLoadBalancingPolicyEventArgs(schedule.NextDelayMs());
                    ReconnectionEvent(this, ea);
                    if (ea.Cancel)
                        break;
                }
                else
                    Thread.Sleep((int)schedule.NextDelayMs());
            }
        }
    }
}
