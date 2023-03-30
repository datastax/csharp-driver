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
    internal class EverywhereStrategy : IReplicationStrategy, IEquatable<EverywhereStrategy>
    {
        private static readonly int HashCode = typeof(EverywhereStrategy).GetHashCode();

        private EverywhereStrategy()
        {
        }

        public static IReplicationStrategy Instance { get; } = new EverywhereStrategy();

        public Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaMap(
            IReadOnlyList<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas,
            int numberOfHostsWithTokens,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            var allHosts = primaryReplicas.Values.Distinct();
            return primaryReplicas.ToDictionary(kvp => kvp.Key, kvp => new HashSet<Host>(allHosts) as ISet<Host>);
        }

        public bool Equals(IReplicationStrategy other)
        {
            return TypedEquals(other as EverywhereStrategy);
        }

        public override bool Equals(object obj)
        {
            return TypedEquals(obj as EverywhereStrategy);
        }

        public bool Equals(EverywhereStrategy other)
        {
            return TypedEquals(other);
        }

        private bool TypedEquals(EverywhereStrategy other)
        {
            return other != null;
        }

        public override int GetHashCode()
        {
            return EverywhereStrategy.HashCode;
        }
    }
}