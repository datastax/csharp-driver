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
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Cassandra
{
    /// <summary>
    ///  A data-center aware Round-robin load balancing policy. <p> This policy
    ///  provides round-robin queries over the node of the local datacenter. It also
    ///  includes in the query plans returned a configurable number of hosts in the
    ///  remote datacenters, but those are always tried after the local nodes. In
    ///  other words, this policy guarantees that no host in a remote datacenter will
    ///  be queried unless no host in the local datacenter can be reached. </p><p> If used
    ///  with a single datacenter, this policy is equivalent to the
    ///  <code>LoadBalancingPolicy.RoundRobin</code> policy, but its DC awareness
    ///  incurs a slight overhead so the <code>LoadBalancingPolicy.RoundRobin</code>
    ///  policy could be prefered to this policy in that case.</p>
    /// </summary>
    public class DCAwareRoundRobinPolicy : ILoadBalancingPolicy
    {

        private readonly string _localDc;
        private readonly int _usedHostsPerRemoteDc;
        Cluster _cluster;
        int _index;

        /// <summary>
        ///  Creates a new datacenter aware round robin policy given the name of the local
        ///  datacenter. <p> The name of the local datacenter provided must be the local
        ///  datacenter name as known by Cassandra. </p><p> The policy created will ignore all
        ///  remote hosts. In other words, this is equivalent to 
        ///  <code>new DCAwareRoundRobinPolicy(localDc, 0)</code>.</p>
        /// </summary>
        /// <param name="localDc"> the name of the local datacenter (as known by Cassandra).</param>
        public DCAwareRoundRobinPolicy(string localDc)
            : this(localDc, 0)
        {
        }

        ///<summary>
        /// Creates a new DCAwareRoundRobin policy given the name of the local
        /// datacenter and that uses the provided number of host per remote
        /// datacenter as failover for the local hosts.
        /// <p>
        /// The name of the local datacenter provided must be the local
        /// datacenter name as known by Cassandra.</p>
        ///</summary>
        /// <param name="localDc"> the name of the local datacenter (as known by
        /// Cassandra).</param>
        /// <param name="usedHostsPerRemoteDc"> the number of host per remote
        /// datacenter that policies created by the returned factory should
        /// consider. Created policies <code>distance</code> method will return a
        /// <code>HostDistance.Remote</code> distance for only <code>
        /// usedHostsPerRemoteDc</code> hosts per remote datacenter. Other hosts
        /// of the remote datacenters will be ignored (and thus no
        /// connections to them will be maintained).</param>
        public DCAwareRoundRobinPolicy(string localDc, int usedHostsPerRemoteDc)
        {
            this._localDc = localDc;
            this._usedHostsPerRemoteDc = usedHostsPerRemoteDc;
        }


        public void Initialize(Cluster cluster)
        {
            this._cluster = cluster;
            this._index = StaticRandom.Instance.Next(cluster.Metadata.AllHosts().Count);
        }

        private string DC(Host host)
        {
            string dc = host.Datacenter;
            return dc ?? "";
        }

        /// <summary>
        ///  Return the HostDistance for the provided host. <p> This policy consider nodes
        ///  in the local datacenter as <code>Local</code>. For each remote datacenter, it
        ///  considers a configurable number of hosts as <code>Remote</code> and the rest
        ///  is <code>Ignored</code>. </p><p> To configure how many host in each remote
        ///  datacenter is considered <code>Remote</code>, see
        ///  <link>#DCAwareRoundRobinPolicy(String, int)</link>.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <code>host</code>.</returns>
        public HostDistance Distance(Host host)
        {
            string dc = DC(host);
            if (dc.Equals(_localDc))
                return HostDistance.Local;

            int ix = 0;
            foreach (var h in _cluster.Metadata.AllHosts())
            {
                if (h.Address.Equals(host.Address))
                {
                    if (ix < _usedHostsPerRemoteDc)
                        return HostDistance.Remote;
                    else
                        return HostDistance.Ignored;
                }
                else if (dc.Equals(DC(h)))
                    ix++;
            }
            return HostDistance.Ignored;
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will always
        ///  try each known host in the local datacenter first, and then, if none of the
        ///  local host is reacheable, will try up to a configurable number of other host
        ///  per remote datacenter. The order of the local node in the returned query plan
        ///  will follow a Round-robin algorithm.</p>
        /// </summary>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        public IEnumerable<Host> NewQueryPlan(Query query)
        {
            var copyOfHosts = (from h in _cluster.Metadata.AllHosts() where h.IsConsiderablyUp select h).ToArray();

            var localHosts = new List<Host>();

            for (int i = 0; i < copyOfHosts.Length; i++)
            {
                var h = copyOfHosts[i];
                if (h.IsConsiderablyUp)
                    if (_localDc.Equals(DC(h)))
                        localHosts.Add(h);
            }

            for (int i = 0; i < localHosts.Count; i++)
            {
                int idxSeed = Interlocked.Increment(ref _index);
                
                // Overflow protection; not theoretically thread safe but should be good enough
                if (idxSeed > int.MaxValue - 10000)
                {
                    Thread.VolatileWrite(ref _index, 0);
                }

                yield return localHosts[idxSeed % localHosts.Count];
            }

            
            var remoteHosts = new List<Host>();
            var ixes = new Dictionary<string, int>();
            for (int i = 0; i < copyOfHosts.Length; i++)
            {
                var h = copyOfHosts[i];
                if (h.IsConsiderablyUp)
                    if (!_localDc.Equals(DC(h)))
                        if (!ixes.ContainsKey(DC(h)) || ixes[DC(h)] < _usedHostsPerRemoteDc)
                        {
                            remoteHosts.Add(h);
                            if (!ixes.ContainsKey(DC(h)))
                                ixes.Add(DC(h), 1);
                            else
                                ixes[DC(h)] = ixes[DC(h)] + 1;
                        }
            }


            for (int i = 0; i < remoteHosts.Count; i++)
            {
                int idxSeed = Interlocked.Increment(ref _index);

                // Overflow protection; not theoretically thread safe but should be good enough
                if (idxSeed > int.MaxValue - 10000)
                {
                    Thread.VolatileWrite(ref _index, 0);
                }

                yield return remoteHosts[idxSeed % remoteHosts.Count];
            }
        }
    }
}
