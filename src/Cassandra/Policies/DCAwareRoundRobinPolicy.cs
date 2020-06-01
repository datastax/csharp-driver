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
using System.Linq;
using Cassandra.SessionManagement;

namespace Cassandra
{
    /// <summary>
    /// A data-center aware Round-robin load balancing policy.
    /// <para>
    /// This policy provides round-robin queries over the node of the local datacenter.
    /// See the comments on <see cref="DCAwareRoundRobinPolicy(string)"/> for more information.
    /// </para>
    /// </summary>
    public class DCAwareRoundRobinPolicy : ILoadBalancingPolicy
    {
        private readonly bool _inferLocalDc;
        private static readonly Logger Logger = new Logger(typeof(DCAwareRoundRobinPolicy));
        private readonly int _maxIndex = int.MaxValue - 10000;
        private volatile List<Host> _hosts;
        private readonly object _hostCreationLock = new object();
        private ICluster _cluster;
        private int _index;

        /// <summary>
        /// Used internally to build the default LBP.
        /// </summary>
        internal DCAwareRoundRobinPolicy() : this(inferLocalDc: false)
        {
        }

        /// <summary>
        /// Used internally to build the default LBP and the DcInferringLBP
        /// </summary>
        internal DCAwareRoundRobinPolicy(bool inferLocalDc)
        {
            _inferLocalDc = inferLocalDc;
        }

        /// <summary>
        ///  <para>
        /// Creates a new datacenter aware round robin policy given the name of the local datacenter.
        /// </para>
        ///  <para>
        /// The name of the local datacenter provided must be the local datacenter name as known by Cassandra. The policy created will ignore all remote hosts.
        /// </para>
        /// </summary>
        /// <param name="localDc"> the name of the local datacenter (as known by Cassandra).</param>
        public DCAwareRoundRobinPolicy(string localDc) : this(inferLocalDc: false)
        {
            if (string.IsNullOrEmpty(localDc))
            {
                throw new ArgumentNullException(nameof(localDc));
            }

            LocalDc = localDc;
        }
        
        /// <summary>
        /// Gets the Local Datacenter. This value is provided in the constructor.
        /// </summary>
        public string LocalDc { get; private set; }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;

            //When the pool changes, it should clear the local cache
            _cluster.HostAdded += _ => ClearHosts();
            _cluster.HostRemoved += _ => ClearHosts();

            LocalDc = cluster.Configuration.LocalDatacenterProvider.DiscoverLocalDatacenter(
                _inferLocalDc, LocalDc);
        }

        /// <summary>
        ///  Return the HostDistance for the provided host. <p> This policy consider nodes
        ///  in the local datacenter as <c>Local</c>. For each remote datacenter, it
        ///  considers a configurable number of hosts as <c>Remote</c> and the rest
        ///  is <c>Ignored</c>. </p><p> To configure how many host in each remote
        ///  datacenter is considered <c>Remote</c>.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <c>host</c>.</returns>
        public HostDistance Distance(Host host)
        {
            var dc = GetDatacenter(host);
            return dc == LocalDc ? HostDistance.Local : HostDistance.Remote;
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will always
        ///  try each known host in the local datacenter first, and then, if none of the
        ///  local host is reachable, will try up to a configurable number of other host
        ///  per remote datacenter. The order of the local node in the returned query plan
        ///  will follow a Round-robin algorithm.</p>
        /// </summary>
        /// <param name="keyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            var startIndex = Interlocked.Increment(ref _index);

            //Simplified overflow protection
            if (startIndex > _maxIndex)
            {
                Interlocked.Exchange(ref _index, 0);
            }

            var hosts = GetHosts();
            //Round-robin through local nodes
            for (var i = 0; i < hosts.Count; i++)
            {
                yield return hosts[(startIndex + i) % hosts.Count];
            }
        }

        private void ClearHosts()
        {
            _hosts = null;
        }

        private string GetDatacenter(Host host)
        {
            var dc = host.Datacenter;
            return dc ?? LocalDc;
        }

        /// <summary>
        /// Gets a tuple containing the list of local and remote nodes
        /// </summary>
        internal List<Host> GetHosts()
        {
            var hosts = _hosts;
            if (hosts != null)
            {
                return hosts;
            }

            lock (_hostCreationLock)
            {
                //Check that if it has been updated since we were waiting for the lock
                hosts = _hosts;
                if (hosts != null)
                {
                    return hosts;
                }

                //shallow copy the nodes
                var allNodes = _cluster.AllHosts().ToArray();

                hosts = allNodes.Where(h => GetDatacenter(h) == LocalDc).ToList();
                _hosts = hosts;
            }
            return hosts;
        }
    }
}
