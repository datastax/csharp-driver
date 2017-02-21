//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Text;

namespace Cassandra
{
    internal class TokenMap
    {
        internal readonly TokenFactory Factory;
        private readonly List<IToken> _ring;
        private readonly Dictionary<string, Dictionary<IToken, ISet<Host>>> _tokenToHostsByKeyspace;
        private readonly Dictionary<IToken, Host> _primaryReplicas;
        private static readonly Logger Logger = new Logger(typeof(ControlConnection));

        internal TokenMap(TokenFactory factory, Dictionary<string, Dictionary<IToken, ISet<Host>>> tokenToHostsByKeyspace, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            Factory = factory;
            _tokenToHostsByKeyspace = tokenToHostsByKeyspace;
            _ring = ring;
            _primaryReplicas = primaryReplicas;
        }
        
        internal IDictionary<IToken, ISet<Host>> GetByKeyspace(string keyspaceName)
        {
            return _tokenToHostsByKeyspace[keyspaceName];
        }

        public static TokenMap Build(string partitioner, ICollection<Host> hosts, ICollection<KeyspaceMetadata> keyspaces)
        {
            var factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
            {
                return null;   
            }

            var primaryReplicas = new Dictionary<IToken, Host>();
            var allSorted = new SortedSet<IToken>();
            var datacenters = new Dictionary<string, DatacenterInfo>();
            foreach (var host in hosts)
            {
                if (host.Datacenter != null)
                {
                    DatacenterInfo dc;
                    if (!datacenters.TryGetValue(host.Datacenter, out dc))
                    {
                        datacenters[host.Datacenter] = dc = new DatacenterInfo();
                    }
                    dc.HostLength++;
                    dc.AddRack(host.Rack);
                }
                foreach (var tokenStr in host.Tokens)
                {
                    try
                    {
                        var token = factory.Parse(tokenStr);
                        allSorted.Add(token);
                        primaryReplicas[token] = host;
                    }
                    catch
                    {
                        Logger.Error(string.Format("Token {0} could not be parsed using {1} partitioner implementation", tokenStr, partitioner));
                    }
                }
            }
            var ring = new List<IToken>(allSorted);
            var tokenToHosts = new Dictionary<string, Dictionary<IToken, ISet<Host>>>(keyspaces.Count);
            var ksTokensCache = new Dictionary<string, Dictionary<IToken, ISet<Host>>>();
            //Only consider nodes that have tokens
            var hostCount = hosts.Count(h => h.Tokens.Any());
            foreach (var ks in keyspaces)
            {
                Dictionary<IToken, ISet<Host>> replicas;
                if (ks.StrategyClass == ReplicationStrategies.SimpleStrategy)
                {
                    replicas = ComputeTokenToReplicaSimple(ks.Replication["replication_factor"], hostCount, ring, primaryReplicas);
                }
                else if (ks.StrategyClass == ReplicationStrategies.NetworkTopologyStrategy)
                {
                    var key = GetReplicationKey(ks.Replication);
                    if (!ksTokensCache.TryGetValue(key, out replicas))
                    {
                        replicas = ComputeTokenToReplicaNetwork(ks.Replication, ring, primaryReplicas, datacenters);
                        ksTokensCache.Add(key, replicas);
                    }
                }
                else
                {
                    //No replication information, use primary replicas
                    replicas = primaryReplicas.ToDictionary(kv => kv.Key, kv => (ISet<Host>)new HashSet<Host>(new [] { kv.Value }));   
                }
                tokenToHosts[ks.Name] = replicas;
            }
            return new TokenMap(factory, tokenToHosts, ring, primaryReplicas);
        }

        private static string GetReplicationKey(IDictionary<string, int> replication)
        {
            var key = new StringBuilder();
            foreach (var kv in replication)
            {
                key.Append(kv.Key);
                key.Append(":");
                key.Append(kv.Value);
                key.Append(",");
            }
            return key.ToString();
        }

        private static Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaNetwork(IDictionary<string, int> replicationFactors,
                                                                                      IList<IToken> ring, 
                                                                                      IDictionary<IToken, Host> primaryReplicas, 
                                                                                      IDictionary<string, DatacenterInfo> datacenters)
        {
            var replicas = new Dictionary<IToken, ISet<Host>>();
            for (var i = 0; i < ring.Count; i++)
            {
                var token = ring[i];
                var replicasByDc = new Dictionary<string, int>();
                var tokenReplicas = new OrderedHashSet<Host>();
                var racksPlaced = new Dictionary<string, HashSet<string>>();
                var skippedHosts = new List<Host>();
                for (var j = 0; j < ring.Count; j++)
                {
                    var replicaIndex = i + j;
                    if (replicaIndex >= ring.Count)
                    {
                        //circle back
                        replicaIndex = replicaIndex % ring.Count;
                    }
                    var h = primaryReplicas[ring[replicaIndex]];
                    var dc = h.Datacenter;
                    int dcRf;
                    if (!replicationFactors.TryGetValue(dc, out dcRf))
                    {
                        continue;
                    }
                    dcRf = Math.Min(dcRf, datacenters[dc].HostLength);
                    int dcReplicas;
                    replicasByDc.TryGetValue(dc, out dcReplicas);
                    //Amount of replicas per dc is equals to the rf or the amount of host in the datacenter
                    if (dcReplicas >= dcRf)
                    {
                        continue;
                    }
                    HashSet<string> racksPlacedInDc;
                    if (!racksPlaced.TryGetValue(dc, out racksPlacedInDc))
                    {
                        racksPlaced[dc] = racksPlacedInDc = new HashSet<string>();
                    }
                    if (h.Rack != null && racksPlacedInDc.Contains(h.Rack) && racksPlacedInDc.Count < datacenters[dc].Racks.Count)
                    {
                        // We already selected a replica for this rack
                        // Skip until replicas in other racks are added
                        if (skippedHosts.Count < dcRf - dcReplicas)
                        {
                            skippedHosts.Add(h);
                        }
                        continue;
                    }
                    dcReplicas += tokenReplicas.Add(h) ? 1 : 0;
                    replicasByDc[dc] = dcReplicas;
                    if (h.Rack != null && racksPlacedInDc.Add(h.Rack) && racksPlacedInDc.Count == datacenters[dc].Racks.Count)
                    {
                        // We finished placing all replicas for all racks in this dc
                        // Add the skipped hosts
                        replicasByDc[dc] += AddSkippedHosts(dc, dcRf, dcReplicas, tokenReplicas, skippedHosts);
                    }
                    if (IsDoneForToken(replicationFactors, replicasByDc, datacenters))
                    {
                        break;
                    }
                }
                replicas[token] = tokenReplicas;
            }
            return replicas;
        }

        internal static int AddSkippedHosts(string dc, int dcRf, int dcReplicas, ISet<Host> tokenReplicas, IList<Host> skippedHosts)
        {
            var counter = 0;
            var length = dcRf - dcReplicas;
            foreach (var h in skippedHosts.Where(h => h.Datacenter == dc))
            {
                tokenReplicas.Add(h);
                if (++counter == length)
                {
                    break;
                }
            }
            return counter;
        }

        internal static bool IsDoneForToken(IDictionary<string, int> replicationFactors,
                                            IDictionary<string, int> replicasByDc,
                                            IDictionary<string, DatacenterInfo> datacenters)
        {
            foreach (var dcName in replicationFactors.Keys)
            {
                DatacenterInfo dc;
                if (!datacenters.TryGetValue(dcName, out dc))
                {
                    // A DC is included in the RF but the DC does not exist in the topology
                    continue;
                }
                var rf = Math.Min(replicationFactors[dcName], dc.HostLength);
                if (rf > 0 && (!replicasByDc.ContainsKey(dcName) || replicasByDc[dcName] < rf))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Converts token-primary to token-replicas
        /// </summary>
        private static Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaSimple(int replicationFactor, int hostCount, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            var rf = Math.Min(replicationFactor, hostCount);
            var tokenToReplicas = new Dictionary<IToken, ISet<Host>>(ring.Count);
            for (var i = 0; i < ring.Count; i++)
            {
                var token = ring[i];
                var replicas = new HashSet<Host>();
                replicas.Add(primaryReplicas[token]);
                var j = 1;
                while (replicas.Count < rf)
                {
                    var nextReplicaIndex = i + j;
                    if (nextReplicaIndex >= ring.Count)
                    {
                        //circle back
                        nextReplicaIndex = nextReplicaIndex % ring.Count;
                    }
                    var nextReplica = primaryReplicas[ring[nextReplicaIndex]];
                    replicas.Add(nextReplica);
                    j++;
                }
                tokenToReplicas.Add(token, replicas);
            }
            return tokenToReplicas;
        }

        public ICollection<Host> GetReplicas(string keyspaceName, IToken token)
        {
            // Find the primary replica
            var i = _ring.BinarySearch(token);
            if (i < 0)
            {
                //no exact match, use closest index
                i = ~i;
                if (i >= _ring.Count)
                {
                    i = 0;
                }
            }
            var closestToken = _ring[i];
            if (keyspaceName != null && _tokenToHostsByKeyspace.ContainsKey(keyspaceName))
            {
                return _tokenToHostsByKeyspace[keyspaceName][closestToken];
            }
            return new Host[] { _primaryReplicas[closestToken] };
        }

        internal class DatacenterInfo
        {
            private readonly HashSet<string> _racks;

            public DatacenterInfo()
            {
                _racks = new HashSet<string>();
            }

            public int HostLength { get; set; }

            public ISet<string> Racks { get { return _racks; } }

            public void AddRack(string name)
            {
                if (name == null)
                {
                    return;
                }
                _racks.Add(name);
            }
        }

        private class OrderedHashSet<T> : ISet<T>
        {
            private readonly HashSet<T> _set;
            private readonly LinkedList<T> _list;

            public int Count { get { return _set.Count; } }

            public bool IsReadOnly { get { return false; } }

            public OrderedHashSet()
            {
                _set = new HashSet<T>();
                _list = new LinkedList<T>();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return _list.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            void ICollection<T>.Add(T item)
            {
                Add(item);
            }

            public void UnionWith(IEnumerable<T> other)
            {
                _set.UnionWith(other);
            }

            public void IntersectWith(IEnumerable<T> other)
            {
                _set.IntersectWith(other);
            }

            public void ExceptWith(IEnumerable<T> other)
            {
                _set.ExceptWith(other);
            }

            public void SymmetricExceptWith(IEnumerable<T> other)
            {
                _set.SymmetricExceptWith(other);
            }

            public bool IsSubsetOf(IEnumerable<T> other)
            {
                return _set.IsSubsetOf(other);
            }

            public bool IsSupersetOf(IEnumerable<T> other)
            {
                return _set.IsSupersetOf(other);
            }

            public bool IsProperSupersetOf(IEnumerable<T> other)
            {
                return _set.IsProperSupersetOf(other);
            }

            public bool IsProperSubsetOf(IEnumerable<T> other)
            {
                return _set.IsProperSubsetOf(other);
            }

            public bool Overlaps(IEnumerable<T> other)
            {
                return _set.Overlaps(other);
            }

            public bool SetEquals(IEnumerable<T> other)
            {
                return _set.SetEquals(other);
            }

            public bool Add(T item)
            {
                var added = _set.Add(item);
                if (added)
                {
                    _list.AddLast(item);
                }
                return added;
            }

            public void Clear()
            {
                _set.Clear();
                _list.Clear();
            }

            public bool Contains(T item)
            {
                return _set.Contains(item);
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                _set.CopyTo(array, arrayIndex);
            }

            public bool Remove(T item)
            {
                var removed = _set.Remove(item);
                if (removed)
                {
                    _list.Remove(item);
                }
                return removed;
            }
        }
    }
}