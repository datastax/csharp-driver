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

using System.Collections.Generic;
using Cassandra.Collections;

namespace Cassandra.MetadataHelpers
{
    internal struct NetworkTopologyTokenMapContext
    {
        public NetworkTopologyTokenMapContext(
            IReadOnlyList<IToken> ring, 
            IReadOnlyDictionary<IToken, Host> primaryReplicas, 
            IReadOnlyDictionary<string, DatacenterInfo> datacenters)
        {
            Ring = ring;
            PrimaryReplicas = primaryReplicas;
            Datacenters = datacenters;
            ReplicasByDc = new Dictionary<string, int>();
            TokenReplicas = new OrderedHashSet<Host>();
            RacksAdded = new Dictionary<string, HashSet<string>>();
            SkippedHosts = new List<Host>();
        }

        public IList<Host> SkippedHosts { get; }

        public IDictionary<string, HashSet<string>> RacksAdded { get; }

        public OrderedHashSet<Host> TokenReplicas { get; }

        public IDictionary<string, int> ReplicasByDc { get; }

        public IReadOnlyList<IToken> Ring { get; }

        public IReadOnlyDictionary<IToken, Host> PrimaryReplicas { get; }

        public IReadOnlyDictionary<string, DatacenterInfo> Datacenters { get; }
    }
}