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
    public class RoundRobinPolicyWithReconnectionRetriesEventArgs : EventArgs
    {
        public long DelayMs { get; private set; }
        public bool Cancel = false;
        public RoundRobinPolicyWithReconnectionRetriesEventArgs(long delayMs)
        {
            this.DelayMs = delayMs;
        }
    }
    public class RoundRobinPolicyWithReconnectionRetries : ILoadBalancingPolicy
    {

        public RoundRobinPolicyWithReconnectionRetries(IReconnectionPolicy reconnectionPolicy)
        {
            this._reconnectionPolicy = reconnectionPolicy;
        }


        public EventHandler<RoundRobinPolicyWithReconnectionRetriesEventArgs> ReconnectionEvent;

        readonly IReconnectionPolicy _reconnectionPolicy;
        Cluster _cluster;
        int _startidx = -1;

        public void Initialize(Cluster cluster)
        {
            this._cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
        }

        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            var schedule = _reconnectionPolicy.NewSchedule();
            while (true)
            {
                List<Host> copyOfHosts = new List<Host>(_cluster.Metadata.AllHosts());
                for (int i = 0; i < copyOfHosts.Count; i++)
                {
                    if (_startidx == -1)
                        _startidx = StaticRandom.Instance.Next(copyOfHosts.Count);

                    _startidx %= copyOfHosts.Count;
                    
                    var h = copyOfHosts[_startidx];

                    _startidx++;

                    if (h.IsConsiderablyUp)
                        yield return h;
                }
                if (ReconnectionEvent != null)
                {
                    var ea = new RoundRobinPolicyWithReconnectionRetriesEventArgs(schedule.NextDelayMs());
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
