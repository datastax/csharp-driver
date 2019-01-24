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
using Cassandra.MetadataHelpers;

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
                        TokenMap.Logger.Error($"Token {tokenStr} could not be parsed using {partitioner} partitioner implementation");
                    }
                }
            }
            var ring = new List<IToken>(allSorted);
            var tokenToHosts = new Dictionary<string, Dictionary<IToken, ISet<Host>>>(keyspaces.Count);
            var ksTokensCache = new Dictionary<IReplicationStrategy, Dictionary<IToken, ISet<Host>>>();
            //Only consider nodes that have tokens
            var hostsWithTokens = hosts.Where(h => h.Tokens.Any()).ToList();
            foreach (var ks in keyspaces)
            {
                Dictionary<IToken, ISet<Host>> replicas;

                if (ks.Strategy == null)
                {
                    replicas = primaryReplicas.ToDictionary(kv => kv.Key, kv => (ISet<Host>) new HashSet<Host>(new[] {kv.Value}));
                }
                else if (!ksTokensCache.TryGetValue(ks.Strategy, out replicas))
                {
                    replicas = ks.Strategy.ComputeTokenToReplicaMap(ks.Replication, ring, primaryReplicas, hostsWithTokens, datacenters);
                    ksTokensCache.Add(ks.Strategy, replicas);
                }

                tokenToHosts[ks.Name] = replicas;
            }
            return new TokenMap(factory, tokenToHosts, ring, primaryReplicas);
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
    }
}