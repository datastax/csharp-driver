// 
//       Copyright DataStax, Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.MetadataHelpers
{
    internal class NetworkTopologyStrategy : IReplicationStrategy, IEquatable<NetworkTopologyStrategy>
    {
        private readonly HashSet<DatacenterReplicationFactor> _replicationFactors;

        public NetworkTopologyStrategy(IDictionary<string, int> replicationFactors)
        {
            this._replicationFactors = new HashSet<DatacenterReplicationFactor>(
                replicationFactors.Select(rf => new DatacenterReplicationFactor(rf.Key, rf.Value)));
        }

        public Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaMap(
            IDictionary<string, int> replicationFactors, 
            IList<IToken> ring, 
            IDictionary<IToken, Host> primaryReplicas,
            ICollection<Host> hosts,
            IDictionary<string, TokenMap.DatacenterInfo> datacenters)
        {
            return ComputeTokenToReplicaNetwork(replicationFactors, ring, primaryReplicas, datacenters);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as NetworkTopologyStrategy);
        }

        public bool Equals(IReplicationStrategy other)
        {
            return Equals(other as NetworkTopologyStrategy);
        }

        public bool Equals(NetworkTopologyStrategy other)
        {
            return other != null && _replicationFactors.SetEquals(other._replicationFactors);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 0;
                foreach (var item in _replicationFactors)
                {
                    hash += item.GetHashCode();
                }
                return 2102697912 * hash;
            }
        }

        private Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaNetwork(
            IDictionary<string, int> replicationFactors,
            IList<IToken> ring, 
            IDictionary<IToken, Host> primaryReplicas, 
            IDictionary<string, TokenMap.DatacenterInfo> datacenters)
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
                    //circle back
                    var replicaIndex = (i + j) % ring.Count;
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

        internal static bool IsDoneForToken(
            IDictionary<string, int> replicationFactors,
            IDictionary<string, int> replicasByDc,
            IDictionary<string, TokenMap.DatacenterInfo> datacenters)
        {
            foreach (var dcName in replicationFactors.Keys)
            {
                TokenMap.DatacenterInfo dc;
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
        
        private static int AddSkippedHosts(string dc, int dcRf, int dcReplicas, ISet<Host> tokenReplicas, IList<Host> skippedHosts)
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