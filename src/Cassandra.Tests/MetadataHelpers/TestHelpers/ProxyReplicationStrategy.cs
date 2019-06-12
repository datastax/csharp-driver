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
using Cassandra.MetadataHelpers;

namespace Cassandra.Tests.MetadataHelpers.TestHelpers
{
    internal class ProxyReplicationStrategy : IReplicationStrategy, IEquatable<ProxyReplicationStrategy>
    {
        private readonly IReplicationStrategy _strategy;

        public ProxyReplicationStrategy(IReplicationStrategy strategy)
        {
            _strategy = strategy;
        }

        public int Calls { get; private set; } = 0;

        public Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaMap(
            IReadOnlyList<IToken> ring,
            IReadOnlyDictionary<IToken, Host> primaryReplicas,
            int hostsWithTokens,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            Calls++;
            return _strategy.ComputeTokenToReplicaMap(ring, primaryReplicas, hostsWithTokens, datacenters);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ProxyReplicationStrategy);
        }

        public bool Equals(IReplicationStrategy other)
        {
            return Equals(other as ProxyReplicationStrategy);
        }

        public bool Equals(ProxyReplicationStrategy other)
        {
            return other != null && _strategy.Equals(other._strategy);
        }

        public override int GetHashCode()
        {
            return _strategy.GetHashCode();
        }
    }
}