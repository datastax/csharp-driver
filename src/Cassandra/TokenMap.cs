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
        private readonly Dictionary<string, Dictionary<IToken, List<Host>>> _tokenToHostsByKeyspace;
        private readonly Dictionary<IToken, Host> _primaryReplicas;
        private static readonly Logger _logger = new Logger(typeof(ControlConnection));

        internal TokenMap(TokenFactory factory, Dictionary<string, Dictionary<IToken, List<Host>>> tokenToHostsByKeyspace, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            Factory = factory;
            _tokenToHostsByKeyspace = tokenToHostsByKeyspace;
            _ring = ring;
            _primaryReplicas = primaryReplicas;
        }

        public static TokenMap Build(string partitioner, Dictionary<Host, HashSet<string>> allTokens, ICollection<KeyspaceMetadata> keyspaces)
        {
            var factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
            {
                return null;   
            }

            var primaryReplicas = new Dictionary<IToken, Host>();
            var allSorted = new SortedSet<IToken>();
            foreach (var entry in allTokens)
            {
                var host = entry.Key;
                foreach (var tokenStr in entry.Value)
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
            var tokenToHosts = new Dictionary<string, Dictionary<IToken, List<Host>>>(keyspaces.Count);
            foreach (var ks in keyspaces)
            {
                Dictionary<IToken, List<Host>> replicas;
                if (ks.StrategyClass == ReplicationStrategies.SimpleStrategy)
                {
                    replicas = ComputeTokenToReplicaSimple(ks.Replication["replication_factor"], ring, primaryReplicas);
                }
                //TODO: Network strategy
                else
                {
                    //No replication information, use primary replicas
                    replicas = primaryReplicas.ToDictionary(kv => kv.Key, kv => new List<Host>(1) {kv.Value});   
                }
                tokenToHosts[ks.Name] = replicas;
            }
            return new TokenMap(factory, tokenToHosts, ring, primaryReplicas);
        }

        /// <summary>
        /// Converts token-primary to token-replicas
        /// </summary>
        private static Dictionary<IToken, List<Host>> ComputeTokenToReplicaSimple(int replicationFactor, List<IToken> ring, Dictionary<IToken, Host> primaryReplicas)
        {
            var rf = Math.Min(replicationFactor, ring.Count);
            var tokenToReplicas = new Dictionary<IToken, List<Host>>(ring.Count);
            for (var i = 0; i < ring.Count; i++)
            {
                var token = ring[i];
                var replicas = new List<Host>(rf);
                replicas.Add(primaryReplicas[token]);
                for (var j = 1; j < rf; j++)
                {
                    var nextReplicaIndex = i + j;
                    if (nextReplicaIndex >= ring.Count)
                    {
                        //circle back
                        nextReplicaIndex = nextReplicaIndex % ring.Count;
                    }
                    var nextReplica = primaryReplicas[ring[nextReplicaIndex]];
                    replicas.Add(nextReplica);
                }
                tokenToReplicas.Add(token, replicas);
            }
            return tokenToReplicas;
        }

        public List<Host> GetReplicas(string keyspaceName, IToken token)
        {
            // Find the primary replica
            var i = _ring.BinarySearch(token);
            if (i < 0)
            {
                //no exact match, use closest index
                i = ~i;
            }
            var closestToken = _ring[i];
            if (keyspaceName != null && _tokenToHostsByKeyspace.ContainsKey(keyspaceName))
            {
                return _tokenToHostsByKeyspace[keyspaceName][closestToken];
            }
            return new List<Host>(1) { _primaryReplicas[closestToken] };
        }
    }
}