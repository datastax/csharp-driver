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
using System.Linq;

namespace Cassandra.MetadataHelpers
{
    internal class NetworkTopologyStrategy : IReplicationStrategy, IEquatable<NetworkTopologyStrategy>
    {
        private readonly SortedSet<DatacenterReplicationFactor> _replicationFactorsSet;
        private readonly IReadOnlyDictionary<string, ReplicationFactor> _replicationFactorsMap;
        private readonly int _hashCode;

        public NetworkTopologyStrategy(IReadOnlyDictionary<string, ReplicationFactor> replicationFactors)
        {
            _replicationFactorsSet = new SortedSet<DatacenterReplicationFactor>(
                replicationFactors.Select(rf => new DatacenterReplicationFactor(rf.Key, rf.Value)));

            _replicationFactorsMap = replicationFactors;
            _hashCode = NetworkTopologyStrategy.ComputeHashCode(_replicationFactorsSet);
        }

        public Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaMap(
            IReadOnlyList<IToken> ring,
            IReadOnlyDictionary<IToken, Host> primaryReplicas,
            int numberOfHostsWithTokens,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            return ComputeTokenToReplicaNetwork(ring, primaryReplicas, datacenters);
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
            return other != null && _replicationFactorsSet.SetEquals(other._replicationFactorsSet);
        }

        private static int ComputeHashCode(IEnumerable<DatacenterReplicationFactor> replicationFactorsSet)
        {
            return Utils.CombineHashCode(replicationFactorsSet);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        private Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaNetwork(
            IReadOnlyList<IToken> ring,
            IReadOnlyDictionary<IToken, Host> primaryReplicas,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            var replicas = new Dictionary<IToken, ISet<Host>>();
            for (var i = 0; i < ring.Count; i++)
            {
                var token = ring[i];
                replicas[token] = ComputeReplicasForToken(ring, primaryReplicas, datacenters, i);
            }

            return replicas;
        }

        private ISet<Host> ComputeReplicasForToken(
            IReadOnlyList<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas, 
            IReadOnlyDictionary<string, DatacenterInfo> datacenters, 
            int i)
        {
            var context = new NetworkTopologyTokenMapContext(ring, primaryReplicas, datacenters);
            for (var j = 0; j < ring.Count; j++)
            {
                // wrap around if necessary
                var replicaIndex = (i + j) % ring.Count;

                var replica = primaryReplicas[ring[replicaIndex]];
                var dc = replica.Datacenter;
                if (!_replicationFactorsMap.TryGetValue(dc, out var dcRfObj))
                {
                    continue;
                }

                var dcRf = Math.Min(dcRfObj.FullReplicas, datacenters[dc].HostLength);
                context.ReplicasByDc.TryGetValue(dc, out var dcAddedReplicas);
                if (dcAddedReplicas >= dcRf)
                {
                    // replication factor for the datacenter has already been satisfied
                    continue;
                }

                var racksAddedInDc = NetworkTopologyStrategy.GetAddedRacksInDatacenter(context, dc);
                if (NetworkTopologyStrategy.ShouldSkipHost(context, replica, racksAddedInDc))
                {
                    NetworkTopologyStrategy.TryAddToSkippedHostsCollection(context, replica, dcRf, dcAddedReplicas);
                    continue;
                }

                NetworkTopologyStrategy.AddReplica(context, replica, dcRf, dcAddedReplicas, racksAddedInDc);

                if (NetworkTopologyStrategy.AreReplicationFactorsSatisfied(_replicationFactorsMap, context.ReplicasByDc, datacenters))
                {
                    break;
                }
            }

            return context.TokenReplicas;
        }

        /// <summary>
        /// Get collection that contains the already added racks in the provided datacenter (<paramref name="dc"/>).
        /// If there's no such collection for the specified datacenter, then create one.
        /// </summary>
        private static HashSet<string> GetAddedRacksInDatacenter(NetworkTopologyTokenMapContext context, string dc)
        {
            if (!context.RacksAdded.TryGetValue(dc, out var racksAddedInDc))
            {
                context.RacksAdded[dc] = racksAddedInDc = new HashSet<string>();
            }

            return racksAddedInDc;
        }

        /// <summary>
        /// Checks whether the host <paramref name="h"/> should be skipped.
        /// </summary>
        private static bool ShouldSkipHost(NetworkTopologyTokenMapContext context, Host h, HashSet<string> racksPlacedInDc)
        {
            var replicaForRackAlreadySelected = h.Rack != null && racksPlacedInDc.Contains(h.Rack);
            var racksMissing = racksPlacedInDc.Count < context.Datacenters[h.Datacenter].Racks.Count;

            return replicaForRackAlreadySelected && racksMissing;
        }

        /// <summary>
        /// This method doesn't guarantee that the host will be added to the skipped hosts collection.
        /// It will depend on whether the collection already has enough hosts to satisfy the replication factor for the host's datacenter.
        /// </summary>
        private static void TryAddToSkippedHostsCollection(NetworkTopologyTokenMapContext context, Host h, int dcRf, int dcAddedReplicas)
        {
            // We already added a replica for this rack, skip until replicas in other racks are added
            var remainingReplicasNeededToSatisfyRf = dcRf - dcAddedReplicas;
            if (context.SkippedHosts.Count < remainingReplicasNeededToSatisfyRf)
            {
                // these replicas will be added in the end after a replica has been selected for every rack
                context.SkippedHosts.Add(h);
            }
        }

        /// <summary>
        /// Adds replica (<paramref name="host"/>) to <see cref="NetworkTopologyTokenMapContext.TokenReplicas"/> of <paramref name="context"/>
        /// and adds skipped hosts if a replica has been added to every rack in datacenter the host's datacenter.
        /// </summary>
        private static void AddReplica(NetworkTopologyTokenMapContext context, Host host, int dcRf, int dcAddedReplicas, HashSet<string> racksPlacedInDc)
        {
            var dc = host.Datacenter;
            if (context.TokenReplicas.Add(host))
            {
                dcAddedReplicas++;
            }
            context.ReplicasByDc[dc] = dcAddedReplicas;

            var rackAdded = host.Rack != null && racksPlacedInDc.Add(host.Rack);
            var allRacksPlacedInDc = racksPlacedInDc.Count == context.Datacenters[dc].Racks.Count;
            if (rackAdded && allRacksPlacedInDc)
            {
                // We finished placing all replicas for all racks in this dc, add the skipped hosts
                context.ReplicasByDc[dc] += NetworkTopologyStrategy.AddSkippedHosts(context, dc, dcRf, dcAddedReplicas);
            }
        }

        /// <summary>
        /// Checks if <paramref name="replicasByDc"/> has enough replicas for each datacenter considering the datacenter's replication factor.
        /// </summary>
        internal static bool AreReplicationFactorsSatisfied(
            IReadOnlyDictionary<string, ReplicationFactor> replicationFactors,
            IDictionary<string, int> replicasByDc,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            foreach (var dcName in replicationFactors.Keys)
            {
                if (!datacenters.TryGetValue(dcName, out var dc))
                {
                    // A DC is included in the RF but the DC does not exist in the topology
                    continue;
                }

                var rf = Math.Min(replicationFactors[dcName].FullReplicas, dc.HostLength);
                if (rf > 0 && (!replicasByDc.ContainsKey(dcName) || replicasByDc[dcName] < rf))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Add replicas that were skipped before to satisfy replication factor
        /// </summary>
        /// <returns>Number of replicas added to <see cref="NetworkTopologyTokenMapContext.TokenReplicas"/> of <paramref name="context"/></returns>
        private static int AddSkippedHosts(NetworkTopologyTokenMapContext context, string dc, int dcRf, int dcReplicas)
        {
            var counter = 0;
            var length = dcRf - dcReplicas;
            foreach (var h in context.SkippedHosts.Where(h => h.Datacenter == dc))
            {
                context.TokenReplicas.Add(h);
                if (++counter == length)
                {
                    break;
                }
            }
            return counter;
        }
    }
}