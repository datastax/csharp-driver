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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Dse.Connections;
using Dse.Connections.Control;
using Dse.ProtocolEvents;
using Dse.SessionManagement;
using Dse.Test.Unit.Connections.TestHelpers;
using Dse.Test.Unit.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.Connections.Control
{
    [TestFixture]
    public class ControlConnectionTests
    {
        private Metadata _metadata;
        private Configuration _config;
        private IInternalCluster _cluster;

        private IEndPointResolver _resolver;
        private FakeConnectionFactory _connectionFactory;
        private IPEndPoint _endpoint1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
        private IPEndPoint _endpoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);

        public ControlConnectionTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        private IProtocolEventDebouncer GetEventDebouncer(Configuration config)
        {
            return new ProtocolEventDebouncer(
                new TaskBasedTimerFactory(),
                TimeSpan.FromMilliseconds(config.MetadataSyncOptions.RefreshSchemaDelayIncrement),
                TimeSpan.FromMilliseconds(config.MetadataSyncOptions.MaxTotalRefreshSchemaDelay));
        }

        private ControlConnection NewInstance(
            IDictionary<IPEndPoint, IRow> rows = null,
            IInternalCluster cluster = null,
            Configuration config = null,
            Metadata metadata = null,
            Action<TestConfigurationBuilder> configBuilderAct = null)
        {
            if (rows == null)
            {
                rows = new Dictionary<IPEndPoint, IRow>
                {
                    {
                        new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042),
                        TestHelper.CreateRow(new Dictionary<string, object>
                        {
                            { "cluster_name", "ut-cluster" },
                            { "data_center", "ut-dc" },
                            { "rack", "ut-rack" },
                            { "tokens", null },
                            { "release_version", "2.2.1-SNAPSHOT" }
                        })
                    }
                };
            }

            if (cluster == null)
            {
                cluster = Mock.Of<IInternalCluster>();
            }

            _cluster = cluster;

            if (config == null)
            {
                var builder = new TestConfigurationBuilder
                {
                    ConnectionFactory = new FakeConnectionFactory(),
                    TopologyRefresherFactory = new FakeTopologyRefresherFactory(rows),
                    SchemaParserFactory = new FakeSchemaParserFactory(),
                    SupportedOptionsInitializerFactory = new FakeSupportedOptionsInitializerFactory(),
                    ProtocolVersionNegotiator = new FakeProtocolVersionNegotiator(),
                    ServerEventsSubscriber = new FakeServerEventsSubscriber()
                };
                configBuilderAct?.Invoke(builder);
                config = builder.Build();
            }

            _config = config;

            if (metadata == null)
            {
                metadata = new Metadata(config);
            }

            _metadata = metadata;

            return new ControlConnection(
                GetEventDebouncer(config),
                ProtocolVersion.MaxSupported,
                config,
                metadata,
                new object[] { "127.0.0.1" });
        }

        [Test]
        public async Task ConnectSetsHost()
        {
            var cc = NewInstance();
            await cc.InitAsync().ConfigureAwait(false);
            Assert.AreEqual("ut-dc", cc.Host.Datacenter);
            Assert.AreEqual("ut-rack", cc.Host.Rack);
            Assert.AreEqual(Version.Parse("2.2.1"), cc.Host.CassandraVersion);
        }

        [Test]
        public void Should_NotAttemptDownOrIgnoredHosts()
        {
            var connectionOpenEnabled = true;
            Action<TestConfigurationBuilder> configAct = builder =>
            {
                builder.SocketOptions = new SocketOptions().SetConnectTimeoutMillis(100).SetReadTimeoutMillis(100);
                builder.Policies = new Dse.Policies(
                    new ClusterUnitTests.FakeHostDistanceLbp(new Dictionary<string, HostDistance>
                    {
                        { "127.0.0.1", HostDistance.Local },
                        { "127.0.0.2", HostDistance.Local },
                        { "127.0.0.3", HostDistance.Ignored },
                        { "127.0.0.4", HostDistance.Local }
                    }),
                    new ConstantReconnectionPolicy(1000),
                    new DefaultRetryPolicy());
                var connFactory = new FakeConnectionFactory(endpoint =>
                {
                    var connection = Mock.Of<IConnection>();
                    Mock.Get(connection).SetupGet(c => c.EndPoint).Returns(endpoint);

                    // ReSharper disable once AccessToModifiedClosure
                    if (!connectionOpenEnabled)
                    {
                        Mock.Get(connection).Setup(c => c.Open())
                            .ThrowsAsync(new SocketException((int)SocketError.ConnectionRefused));
                    }

                    return connection;
                });
                builder.ConnectionFactory = connFactory;
            };
            var localHost = IPAddress.Parse("127.0.0.1");
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var hostAddress4 = IPAddress.Parse("127.0.0.4");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", localHost}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress4}, { "data_center", "ut-dc3" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}}
            });
            var rowsWithIp = new Dictionary<IPEndPoint, IRow>
            {
                { new IPEndPoint(localHost, 9042), rows.ElementAt(0) },
                { new IPEndPoint(hostAddress2, 9042), rows.ElementAt(1) },
                { new IPEndPoint(hostAddress3, 9042), rows.ElementAt(2) },
                { new IPEndPoint(hostAddress4, 9042), rows.ElementAt(3) },
            };
            using (var cc = NewInstance(rowsWithIp, configBuilderAct: configAct))
            {
                var metadata = _metadata;
                var config = _config;
                var cluster = _cluster;
                cc.InitAsync().GetAwaiter().GetResult();
                Assert.AreEqual(4, metadata.AllHosts().Count);
                var host2 = metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
                Assert.NotNull(host2);
                host2.SetDown();
                var host3 = metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
                Assert.NotNull(host3);

                Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(() => metadata.AllHosts());
                config.Policies.LoadBalancingPolicy.Initialize(cluster);

                connectionOpenEnabled = false;

                var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => cc.Reconnect());
                CollectionAssert.AreEquivalent(new[] { "127.0.0.1", "127.0.0.4" }, ex.Errors.Keys.Select(e => e.Address.ToString()));
            }
        }

        [Test]
        public void Should_ResolveContactPointsAndAttemptEveryOne_When_ContactPointResolutionReturnsMultiple()
        {
            var target = CreateForContactPointTest();

            var noHostAvailableException = Assert.ThrowsAsync<NoHostAvailableException>(() => target.InitAsync());

            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("cp1"), Times.Once);
            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("127.0.0.1"), Times.Once);
            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("cp2"), Times.Once);
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint1].Count);
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint2].Count);
        }

        private IControlConnection CreateForContactPointTest()
        {
            _connectionFactory = new FakeConnectionFactory();
            _resolver = Mock.Of<IEndPointResolver>();
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("127.0.0.1")).ReturnsAsync(
                new List<IConnectionEndPoint> { new ConnectionEndPoint(_endpoint1, null) });
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("cp2")).ReturnsAsync(
                new List<IConnectionEndPoint> { new ConnectionEndPoint(_endpoint2, null) });
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("cp1")).ReturnsAsync(
                new List<IConnectionEndPoint>
                {
                    new ConnectionEndPoint(_endpoint1, null),
                    new ConnectionEndPoint(_endpoint2, null)
                });
            var config = new TestConfigurationBuilder
            {
                EndPointResolver = _resolver,
                ConnectionFactory = _connectionFactory
            }.Build();
            return new ControlConnection(
                new ProtocolEventDebouncer(
                    new FakeTimerFactory(), TimeSpan.Zero, TimeSpan.Zero),
                ProtocolVersion.V3,
                config,
                new Metadata(config),
                new List<object> { "cp1", "cp2", "127.0.0.1" });
        }
    }
}