//
//       Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

using Cassandra.Connections.Control;
using Cassandra.MetadataHelpers;

namespace Cassandra.Tests.MetadataHelpers.TestHelpers
{
    internal class FakeSchemaParserFactory : ISchemaParserFactory
    {
        public ISchemaParser Create(
            Version cassandraVersion, IInternalMetadata parent, Func<string, string, Task<UdtColumnInfo>> udtResolver, ISchemaParser currentInstance = null)
        {
            var keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>();

            // unique configurations
            keyspaces.AddOrUpdate("ks1", FakeSchemaParserFactory.CreateSimpleKeyspace("ks1", 2), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks4", FakeSchemaParserFactory.CreateNetworkTopologyKeyspace("ks4", new Dictionary<string, string> { { "dc1", "2" }, { "dc2", "2" } }), (s, keyspaceMetadata) => keyspaceMetadata);

            return new FakeSchemaParser(keyspaces);
        }

        public static KeyspaceMetadata CreateSimpleKeyspace(string name, int replicationFactor, IReplicationStrategyFactory factory = null)
        {
            return new KeyspaceMetadata(
                null,
                name,
                true,
                ReplicationStrategies.SimpleStrategy,
                new Dictionary<string, string> { { "replication_factor", replicationFactor.ToString() } },
                factory ?? new ReplicationStrategyFactory());
        }

        public static KeyspaceMetadata CreateNetworkTopologyKeyspace(string name, IDictionary<string, string> replicationFactors, IReplicationStrategyFactory factory = null)
        {
            return new KeyspaceMetadata(
                null,
                name,
                true,
                ReplicationStrategies.NetworkTopologyStrategy,
                replicationFactors,
                factory ?? new ReplicationStrategyFactory());
        }
    }
}