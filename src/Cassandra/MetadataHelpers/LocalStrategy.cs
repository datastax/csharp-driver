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
    internal class LocalStrategy : IReplicationStrategy, IEquatable<LocalStrategy>
    {
        private static readonly int HashCode = typeof(LocalStrategy).GetHashCode();

        private LocalStrategy()
        {
        }

        public static IReplicationStrategy Instance { get; } = new LocalStrategy();

        public Dictionary<IToken, ISet<Host>> ComputeTokenToReplicaMap(
            IReadOnlyList<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas,
            int numberOfHostsWithTokens,
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            return primaryReplicas.ToDictionary(kvp => kvp.Key, kvp => new HashSet<Host> { kvp.Value } as ISet<Host>);
        }

        public bool Equals(IReplicationStrategy other)
        {
            return TypedEquals(other as LocalStrategy);
        }

        public override bool Equals(object obj)
        {
            return TypedEquals(obj as LocalStrategy);
        }

        public bool Equals(LocalStrategy other)
        {
            return TypedEquals(other);
        }

        private bool TypedEquals(LocalStrategy other)
        {
            return other != null;
        }

        public override int GetHashCode()
        {
            return LocalStrategy.HashCode;
        }
    }
}