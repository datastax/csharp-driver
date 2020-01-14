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

﻿using System.Collections.Generic;
using System.Threading;
using System.Linq;


namespace Cassandra
{
    /// <summary>
    ///  A Round-robin load balancing policy. 
    /// <para> This policy queries nodes in a
    ///  round-robin fashion. For a given query, if an host fail, the next one
    ///  (following the round-robin order) is tried, until all hosts have been tried.
    ///  </para>
    /// <para> This policy is not datacenter aware and will include every known
    ///  Cassandra host in its round robin algorithm. If you use multiple datacenter
    ///  this will be inefficient and you will want to use the
    ///  <see cref="DCAwareRoundRobinPolicy"/> load balancing policy instead.
    /// </para>
    /// </summary>
    public class RoundRobinPolicy : ILoadBalancingPolicy
    {
        ICluster _cluster;
        int _index;

        public void Initialize(ICluster cluster)
        {
            this._cluster = cluster;
        }

        /// <summary>
        ///  Return the HostDistance for the provided host. <p> This policy consider all
        ///  nodes as local. This is generally the right thing to do in a single
        ///  datacenter deployment. If you use multiple datacenter, see
        ///  <link>DCAwareRoundRobinPolicy</link> instead.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <c>host</c>.</returns>
        public HostDistance Distance(Host host)
        {
            return HostDistance.Local;
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will try each
        ///  known host of the cluster. Upon each call to this method, the ith host of the
        ///  plans returned will cycle over all the host of the cluster in a round-robin
        ///  fashion.</p>
        /// </summary>
        /// <param name="keyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            //shallow copy the all hosts
            var hosts = (from h in _cluster.AllHosts() select h).ToArray();
            var startIndex = Interlocked.Increment(ref _index);

            //Simplified overflow protection
            if (startIndex > int.MaxValue - 10000)
            {
                Interlocked.Exchange(ref _index, 0);
            }

            for (var i = 0; i < hosts.Length; i++)
            {
                yield return hosts[(startIndex + i) % hosts.Length];
            }
        }
    }
}
