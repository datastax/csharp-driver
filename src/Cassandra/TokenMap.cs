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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Cassandra.Connections.Control;
using Cassandra.MetadataHelpers;

namespace Cassandra
{
    internal class TokenMap : IReadOnlyTokenMap
    {
        internal readonly TokenFactory Factory;

        // should be IReadOnly but BinarySearch method is not exposed in the interface
        private readonly List<IToken> _ring;

        private readonly IReadOnlyDictionary<IToken, Host> _primaryReplicas;
        private readonly IReadOnlyDictionary<string, DatacenterInfo> _datacenters;
        private readonly int _numberOfHostsWithTokens;
        private readonly ConcurrentDictionary<string, IReadOnlyDictionary<IToken, ISet<Host>>> _tokenToHostsByKeyspace;
        private readonly ConcurrentDictionary<IReplicationStrategy, IReadOnlyDictionary<IToken, ISet<Host>>> _keyspaceTokensCache;
        private static readonly Logger Logger = new Logger(typeof(ControlConnection));

        internal TokenMap(
            TokenFactory factory, 
            IReadOnlyDictionary<string, IReadOnlyDictionary<IToken, ISet<Host>>> tokenToHostsByKeyspace, 
            List<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas, 
            IReadOnlyDictionary<IReplicationStrategy, IReadOnlyDictionary<IToken, ISet<Host>>> keyspaceTokensCache, 
            IReadOnlyDictionary<string, DatacenterInfo> datacenters, 
            int numberOfHostsWithTokens)
        {
            Factory = factory;
            _tokenToHostsByKeyspace = new ConcurrentDictionary<string, IReadOnlyDictionary<IToken, ISet<Host>>>(tokenToHostsByKeyspace);
            _ring = ring;
            _primaryReplicas = primaryReplicas;
            _keyspaceTokensCache = new ConcurrentDictionary<IReplicationStrategy, IReadOnlyDictionary<IToken, ISet<Host>>>(keyspaceTokensCache);
            _datacenters = datacenters;
            _numberOfHostsWithTokens = numberOfHostsWithTokens;
        }

        public IReadOnlyDictionary<IToken, ISet<Host>> GetByKeyspace(string keyspaceName)
        {
            _tokenToHostsByKeyspace.TryGetValue(keyspaceName, out var value);
            return value;
        }

        public void UpdateKeyspace(KeyspaceMetadata ks)
        {
            var sw = new Stopwatch();
            sw.Start();

            TokenMap.UpdateKeyspace(
                ks, _tokenToHostsByKeyspace, _ring, _primaryReplicas, _keyspaceTokensCache, _datacenters, _numberOfHostsWithTokens);

            sw.Stop();
            TokenMap.Logger.Info(
                "Finished updating TokenMap for the '{0}' keyspace. It took {1:0} milliseconds.", 
                ks.Name,
                sw.Elapsed.TotalMilliseconds);
        }

        public ICollection<Host> GetReplicas(string keyspaceName, IToken token)
        {
            IReadOnlyList<IToken> readOnlyRing = _ring;

            // Find the primary replica
            var i = _ring.BinarySearch(token);
            if (i < 0)
            {
                //no exact match, use closest index
                i = ~i;
                if (i >= readOnlyRing.Count)
                {
                    i = 0;
                }
            }

            var closestToken = readOnlyRing[i];
            if (keyspaceName != null && _tokenToHostsByKeyspace.ContainsKey(keyspaceName))
            {
                return _tokenToHostsByKeyspace[keyspaceName][closestToken];
            }

            TokenMap.Logger.Warning("An attempt to obtain the replicas for a specific token was made but the replicas collection " +
                                    "wasn't computed for this keyspace: {0}. Returning the primary replica for the provided token.", keyspaceName);
            return new[] { _primaryReplicas[closestToken] };
        }

        public static TokenMap Build(string partitioner, ICollection<Host> hosts, ICollection<KeyspaceMetadata> keyspaces)
        {
            var sw = new Stopwatch();
            sw.Start();
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
                    if (!datacenters.TryGetValue(host.Datacenter, out var dc))
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
                    catch (Exception ex)
                    {
                        TokenMap.Logger.Error($"Token {tokenStr} could not be parsed using {partitioner} partitioner implementation. Exception: {ex}");
                    }
                }
            }
            var ring = new List<IToken>(allSorted);
            var tokenToHosts = new Dictionary<string, IReadOnlyDictionary<IToken, ISet<Host>>>(keyspaces.Count);
            var ksTokensCache = new Dictionary<IReplicationStrategy, IReadOnlyDictionary<IToken, ISet<Host>>>();
            //Only consider nodes that have tokens
            var numberOfHostsWithTokens = hosts.Count(h => h.Tokens.Any());
            foreach (var ks in keyspaces)
            {
                TokenMap.UpdateKeyspace(ks, tokenToHosts, ring, primaryReplicas, ksTokensCache, datacenters, numberOfHostsWithTokens);
            }

            sw.Stop();
            TokenMap.Logger.Info(
                "Finished building TokenMap for {0} keyspaces and {1} hosts. It took {2:0} milliseconds.", 
                keyspaces.Count, 
                hosts.Count, 
                sw.Elapsed.TotalMilliseconds);
            return new TokenMap(factory, tokenToHosts, ring, primaryReplicas, ksTokensCache, datacenters, numberOfHostsWithTokens);
        }

        private static void UpdateKeyspace(
            KeyspaceMetadata ks,
            IDictionary<string, IReadOnlyDictionary<IToken, ISet<Host>>> tokenToHostsByKeyspace, 
            IReadOnlyList<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas, 
            IDictionary<IReplicationStrategy, IReadOnlyDictionary<IToken, ISet<Host>>> keyspaceTokensCache, 
            IReadOnlyDictionary<string, DatacenterInfo> datacenters, 
            int numberOfHostsWithTokens)
        {
            IReadOnlyDictionary<IToken, ISet<Host>> replicas;
            if (ks.Strategy == null)
            {
                replicas = primaryReplicas.ToDictionary(kv => kv.Key, kv => (ISet<Host>)new HashSet<Host>(new[] { kv.Value }));
            }
            else if (!keyspaceTokensCache.TryGetValue(ks.Strategy, out replicas))
            {
                replicas = ks.Strategy.ComputeTokenToReplicaMap(ring, primaryReplicas, numberOfHostsWithTokens, datacenters);
                keyspaceTokensCache[ks.Strategy] = replicas;
            }

            tokenToHostsByKeyspace[ks.Name] = replicas;
        }

        public void RemoveKeyspace(string name)
        {
            _tokenToHostsByKeyspace.TryRemove(name, out _);
        }
    }
}
