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
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections.TestHelpers;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class GraphProtocolResolverTests
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
            var resolver = new GraphProtocolResolver();

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

            var actual = resolver.GetDefaultGraphProtocol(session, statement, options);

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
            var resolver = new GraphProtocolResolver();

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

            var actual = resolver.GetDefaultGraphProtocol(session, statement, options);

            Assert.AreEqual(expected, actual);
        }

        private ISession MockSession(string keyspace, bool coreEngine)
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

            var session = Mock.Of<ISession>();
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