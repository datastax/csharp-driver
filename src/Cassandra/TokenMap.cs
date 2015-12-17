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
using System.Collections.Generic;
using System.Net;
using System.Linq;

namespace Cassandra
{
    internal class TokenMap
    {
        internal readonly TokenFactory Factory;
        private readonly List<IToken> _ring;
        private readonly Dictionary<string, Dictionary<IToken, HashSet<Host>>> _tokenToHostsByKeyspace;
        private readonly Dictionary<IToken, Host> _primaryReplicas;
        private static readonly Logger _logger = new Logger(typeof(ControlConnection));

        internal TokenMap(TokenFactory factory, Dictionary<string, Dictionary<IToken, HashSet<Host>>> tokenToHostsByKeyspace, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            Factory = factory;
            _tokenToHostsByKeyspace = tokenToHostsByKeyspace;
            _ring = ring;
            _primaryReplicas = primaryReplicas;
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
            var datacenters = new Dictionary<string, int>();
            foreach (var host in hosts)
            {
                if (host.Datacenter != null)
                {
                    if (!datacenters.ContainsKey(host.Datacenter))
                    {
                        datacenters[host.Datacenter] = 1;
                    }
                    else
                    {
                        datacenters[host.Datacenter]++;
                    }
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
                        _logger.Error(String.Format("Token {0} could not be parsed using {1} partitioner implementation", tokenStr, partitioner));
                    }
                }
            }
            var ring = new List<IToken>(allSorted);
            var tokenToHosts = new Dictionary<string, Dictionary<IToken, HashSet<Host>>>(keyspaces.Count);
            //Only consider nodes that have tokens
            var hostCount = hosts.Count(h => h.Tokens.Any());
            foreach (var ks in keyspaces)
            {
                Dictionary<IToken, HashSet<Host>> replicas;
                if (ks.StrategyClass == ReplicationStrategies.SimpleStrategy)
                {
                    replicas = ComputeTokenToReplicaSimple(ks.Replication["replication_factor"], hostCount, ring, primaryReplicas);
                }
                else if (ks.StrategyClass == ReplicationStrategies.NetworkTopologyStrategy)
                {
                    replicas = ComputeTokenToReplicaNetwork(ks.Replication, ring, primaryReplicas, datacenters);
                }
                else
                {
                    //No replication information, use primary replicas
                    replicas = primaryReplicas.ToDictionary(kv => kv.Key, kv => new HashSet<Host>(new [] { kv.Value }));   
                }
                tokenToHosts[ks.Name] = replicas;
            }
            return new TokenMap(factory, tokenToHosts, ring, primaryReplicas);
        }

        private static Dictionary<IToken, HashSet<Host>> ComputeTokenToReplicaNetwork(IDictionary<string, int> replicationFactors, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas, Dictionary<string, int> datacenters)
        {
            var replicas = new Dictionary<IToken, HashSet<Host>>();
            for (var i = 0; i < ring.Count; i++)
            {
                var token = ring[i];
                var replicasByDc = new Dictionary<string, int>();
                var tokenReplicas = new HashSet<Host>();
                for (var j = 0; j < ring.Count; j++)
                {
                    var replicaIndex = i + j;
                    if (replicaIndex >= ring.Count)
                    {
                        //circle back
                        replicaIndex = replicaIndex % ring.Count;
                    }
                    var h = primaryReplicas[ring[replicaIndex]];
                    var dcRf = 0;
                    if (!replicationFactors.TryGetValue(h.Datacenter, out dcRf))
                    {
                        continue;
                    }
                    dcRf = Math.Min(dcRf, datacenters[h.Datacenter]);
                    var dcReplicas = 0;
                    replicasByDc.TryGetValue(h.Datacenter, out dcReplicas);
                    //Amount of replicas per dc is equals to the rf or the amount of host in the datacenter
                    if (dcReplicas >= dcRf)
                    {
                        continue;
                    }
                    
                    // On a cluster running virtual nodes, one host can own 2 continuous ranges, but these
                    // are not replicas (NetworkTopologyStrategy.Java - calculateNaturalEndpoints)
                    if(tokenReplicas.Contains(h))
                        continue;

                    replicasByDc[h.Datacenter] = dcReplicas + 1;
                    tokenReplicas.Add(h);
                    
                    if (IsDoneForToken(replicationFactors, replicasByDc, datacenters))
                    {
                        break;
                    }
                }
                replicas[token] = tokenReplicas;
            }
            return replicas;
        }

        internal static bool IsDoneForToken(IDictionary<string, int> replicationFactors, Dictionary<string, int> replicasByDc, Dictionary<string, int> datacenters)
        {
            foreach (var dc in replicationFactors.Keys)
            {
                var rf = Math.Min(replicationFactors[dc], datacenters.ContainsKey(dc) ? datacenters[dc] : 0);
                if (rf == 0)
                {
                    //A DC is included in the RF but the DC does not exist
                    continue;
                }
                if (!replicasByDc.ContainsKey(dc) || replicasByDc[dc] < rf)
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Converts token-primary to token-replicas
        /// </summary>
        private static Dictionary<IToken, HashSet<Host>> ComputeTokenToReplicaSimple(int replicationFactor, int hostCount, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            var rf = Math.Min(replicationFactor, hostCount);
            var tokenToReplicas = new Dictionary<IToken, HashSet<Host>>(ring.Count);
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
    }
}