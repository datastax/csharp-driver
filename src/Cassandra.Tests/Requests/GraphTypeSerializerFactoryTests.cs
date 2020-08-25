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
using System.Linq;
using System.Net;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization.Graph;
using Cassandra.Serialization.Graph.GraphSON2.Tinkerpop;
using Cassandra.Serialization.Graph.GraphSON3.Dse;
using Cassandra.Serialization.Graph.GraphSON3.Tinkerpop;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections.TestHelpers;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class GraphTypeSerializerFactoryTests
    {
        [TestCase(GraphProtocol.GraphSON3, true, null, null)]
        [TestCase(GraphProtocol.GraphSON3, true, GraphProtocol.GraphSON2, GraphProtocol.GraphSON1)]
        [TestCase(GraphProtocol.GraphSON1, false, null, null)]
        [TestCase(GraphProtocol.GraphSON1, false, GraphProtocol.GraphSON3, GraphProtocol.GraphSON2)]
        [Test]
        public void Should_IgnoreProtocolOnOptionsAndStatement_When_ResolvingDefaultGraphProtocol(
            GraphProtocol expected, bool coreGraph, GraphProtocol? optionsLevel, GraphProtocol? statementLevel)
        {
            var ks = $"ks{Guid.NewGuid().GetHashCode()}";
            var session = MockSession(ks, coreGraph);
            var factory = new GraphTypeSerializerFactory();

            var options = new GraphOptions().SetName(ks);
            IGraphStatement statement = new SimpleGraphStatement("");

            if (optionsLevel.HasValue)
            {
                options = options.SetGraphProtocolVersion(optionsLevel.Value);
            }
            else
            {
                Assert.IsNull(options.GraphProtocolVersion);
            }

            if (statementLevel.HasValue)
            {
                statement = statement.SetGraphProtocolVersion(statementLevel.Value);
            }
            else
            {
                Assert.IsNull(statement.GraphProtocolVersion);
            }

            var actual = factory.GetDefaultGraphProtocol(session, statement, options);

            Assert.AreEqual(expected, actual);
        }
        
        [TestCase(GraphProtocol.GraphSON1, "ks2", null, null)]
        [TestCase(GraphProtocol.GraphSON3, "ks2", "ks2", null)]
        [TestCase(GraphProtocol.GraphSON3, "ks2", null, "ks2")]
        [TestCase(GraphProtocol.GraphSON3, "ks2", "ks2", "ks2")]
        [TestCase(GraphProtocol.GraphSON3, "ks2", "ks1", "ks2")]
        [TestCase(GraphProtocol.GraphSON1, "ks1", "ks2", null)]
        [TestCase(GraphProtocol.GraphSON1, "ks1", null, "ks2")]
        [TestCase(GraphProtocol.GraphSON1, "ks1", "ks2", "ks2")]
        [TestCase(GraphProtocol.GraphSON1, "ks1", "ks1", "ks2")]
        [Test]
        public void Should_UseStatementGraphNameBeforeOptions_When_ResolvingDefaultGraphProtocol(
            GraphProtocol expected, string coreGraphName, string optionsLevel, string statementLevel)
        {
            var session = MockSession(coreGraphName, true);
            var factory = new GraphTypeSerializerFactory();

            var options = new GraphOptions();
            IGraphStatement statement = new SimpleGraphStatement("");

            if (optionsLevel != null)
            {
                options = options.SetName(optionsLevel);
            }
            else
            {
                Assert.IsNull(options.Name);
            }

            if (statementLevel != null)
            {
                statement = statement.SetGraphName(statementLevel);
            }
            else
            {
                Assert.IsNull(statement.GraphName);
            }

            var actual = factory.GetDefaultGraphProtocol(session, statement, options);

            Assert.AreEqual(expected, actual);
        }
        
        [Test]
        public void Should_CacheSerializerInstances_When_SameParametersAreUsed()
        {
            var session = MockSession("graph", true);
            var session2 = MockSession("graph", true);
            var factory = new GraphTypeSerializerFactory();

            var deserializers1 = new Dictionary<string, IGraphSONDeserializer>
            { { "byte", new ByteBufferDeserializer() } };
            
            var deserializers2 = new Dictionary<string, IGraphSONDeserializer>
            { { "duration", new Duration2Serializer() } };

            var serializers1 = new Dictionary<Type, IGraphSONSerializer>
            { { typeof(byte[]), new ByteBufferSerializer() } };

            var serializers2 = new Dictionary<Type, IGraphSONSerializer>
            { { typeof(Duration), new Duration2Serializer() } };

            var deserializersByProtocol = new Dictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>>
            {
                { GraphProtocol.GraphSON3, deserializers1 },
                { GraphProtocol.GraphSON2, deserializers2 }
            };
            var deserializersByProtocolEqual = new Dictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>>
            {
                { GraphProtocol.GraphSON3, deserializers1 },
                { GraphProtocol.GraphSON2, deserializers2 }
            };
            var deserializersByProtocol2 = new Dictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>>
            {
                { GraphProtocol.GraphSON3, deserializers2 },
                { GraphProtocol.GraphSON2, deserializers1 }
            };
            var serializersByProtocol = new Dictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>>
            {
                { GraphProtocol.GraphSON3, serializers1 },
                { GraphProtocol.GraphSON2, serializers2 }
            };
            var serializersByProtocolEqual = new Dictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>>
            {
                { GraphProtocol.GraphSON3, serializers1 },
                { GraphProtocol.GraphSON2, serializers2 }
            };
            var serializersByProtocol2 = new Dictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>>
            {
                { GraphProtocol.GraphSON3, serializers2 },
                { GraphProtocol.GraphSON2, serializers1 }
            };

            var instances = new List<IGraphTypeSerializer>
            {
                // first 2 use the same parameters
                factory.CreateSerializer(session, deserializersByProtocol, serializersByProtocol, GraphProtocol.GraphSON2, true),
                factory.CreateSerializer(session, deserializersByProtocolEqual, serializersByProtocolEqual, GraphProtocol.GraphSON2, true),

                // the remaining instances all use different parameters
                factory.CreateSerializer(session, deserializersByProtocol, serializersByProtocol, GraphProtocol.GraphSON3, true),
                factory.CreateSerializer(session, deserializersByProtocol, serializersByProtocol, GraphProtocol.GraphSON2, false),
                factory.CreateSerializer(
                    session, deserializersByProtocol, serializersByProtocol2, GraphProtocol.GraphSON2,
                    true),
                factory.CreateSerializer(session, deserializersByProtocol2, serializersByProtocol, GraphProtocol.GraphSON2, true),
                factory.CreateSerializer(session2, deserializersByProtocol, serializersByProtocol, GraphProtocol.GraphSON2, true),
                factory.CreateSerializer(session, null, serializersByProtocol, GraphProtocol.GraphSON2, true),
                factory.CreateSerializer(session, deserializersByProtocol, null, GraphProtocol.GraphSON2, true)
            };

            for (var i = 0; i < instances.Count; i++)
            {
                for (var j = 0; j < instances.Count; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }

                    // if we're comparing the first and second instances, they are supposed to be the same
                    // otherwise it's supposed to be different
                    if (i + j == 1)
                    {
                        Assert.AreSame(instances[i], instances[j], $"i: {i}, j: {j}");
                    }
                    else
                    {
                        Assert.AreNotSame(instances[i], instances[j], $"i: {i}, j: {j}");
                    }
                }
            }
        }

        private IInternalSession MockSession(string keyspace, bool coreEngine)
        {
            var keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>();

            // unique configurations
            keyspaces.AddOrUpdate(
                keyspace, 
                FakeSchemaParserFactory.CreateSimpleKeyspace(keyspace, 2, graphEngine: coreEngine ? "Core" : null), 
                (s, keyspaceMetadata) => keyspaceMetadata);
            
            var schemaParser = new FakeSchemaParser(keyspaces);
            var config = new TestConfigurationBuilder
            {
                ConnectionFactory = new FakeConnectionFactory()
            }.Build();
            
            var metadata = new Metadata(config, schemaParser) {Partitioner = "Murmur3Partitioner"};

            var cluster = Mock.Of<ICluster>();
            Mock.Get(cluster).SetupGet(c => c.Metadata).Returns(metadata);

            var session = Mock.Of<IInternalSession>();
            Mock.Get(session).SetupGet(s => s.Cluster).Returns(cluster);
            
            metadata.ControlConnection = new ControlConnection(
                Mock.Of<IInternalCluster>(),
                new ProtocolEventDebouncer(new TaskBasedTimerFactory(), TimeSpan.FromMilliseconds(20), TimeSpan.FromSeconds(100)), 
                ProtocolVersion.V3, 
                config, 
                metadata,
                new List<IContactPoint>
                {
                    new IpLiteralContactPoint(IPAddress.Parse("127.0.0.1"), config.ProtocolOptions, config.ServerNameResolver)
                });
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.1"), 9042));
            metadata.Hosts.First().SetInfo(new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "tokens", new [] { "100" } },
                { "release_version", "3.11.1" }
            }));
            metadata.RebuildTokenMapAsync(false, true).GetAwaiter().GetResult();
            Assert.IsNotNull(metadata.GetKeyspace(keyspace));
            return session;
        }
    }
}