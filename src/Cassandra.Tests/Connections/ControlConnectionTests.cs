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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ProtocolEvents;
using Cassandra.SessionManagement;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class ControlConnectionTests
    {
        //private FakeConnectionFactory _connectionFactory;
        private IPEndPoint _endpoint1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
        private IPEndPoint _endpoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);
        //private TestContactPoint _cp1;
        //private TestContactPoint _cp2;
        //private TestContactPoint _localhost;
        
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
            IInternalCluster cluster, 
            Configuration config, 
            Metadata metadata, 
            ITopologyRefresher topologyRefresher = null)
        {
            if (topologyRefresher == null)
            {
                topologyRefresher = Mock.Of<ITopologyRefresher>();
            }

            return new ControlConnection(
                cluster, 
                GetEventDebouncer(config), 
                ProtocolVersion.MaxSupported, 
                config, 
                metadata, 
                topologyRefresher, 
                new List<IContactPoint>
                {
                    new IpLiteralContactPoint(
                        IPAddress.Parse("127.0.0.1"), 
                        config.ProtocolOptions, 
                        config.ServerNameResolver)
                });
        }

        [Test]
        public async Task ConnectSetsHost()
        {
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, { "data_center", "ut-dc" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1-SNAPSHOT"}
            });
            var config = new TestConfigurationBuilder
            {
                ConnectionFactory = new FakeConnectionFactory(),
                TopologyRefresherFactory = new FakeTopologyRefresherFactory(new Dictionary<IPEndPoint, IRow>
                {
                    { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042), row}
                }),
                SchemaParserFactory = new FakeSchemaParserFactory()
            }.Build();
            var metadata = new Metadata(config);
            var cc = NewInstance(Mock.Of<IInternalCluster>(), config, metadata);
            await cc.InitAsync().ConfigureAwait(false);
            Assert.AreEqual("ut-dc", cc.Host.Datacenter);
            Assert.AreEqual("ut-rack", cc.Host.Rack);
            Assert.AreEqual(Version.Parse("2.2.1"), cc.Host.CassandraVersion);
        }

        //[Test]
        //public void UpdatePeersInfoModifiesPool()
        //{
        //    var config = new Configuration();
        //    var metadata = new Metadata(config);
        //    var cc = NewInstance(Mock.Of<IInternalCluster>(), config, metadata);
        //    cc.Host = TestHelper.CreateHost("127.0.0.1");
        //    metadata.AddHost(cc.Host.Address);
        //    var hostAddress2 = IPAddress.Parse("127.0.0.2");
        //    var hostAddress3 = IPAddress.Parse("127.0.0.3");
        //    var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
        //    {
        //        new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
        //        new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}}
        //    });
        //    cc.UpdatePeersInfo(rows, cc.Host);
        //    Assert.AreEqual(3, metadata.AllHosts().Count);
        //    //using rpc_address
        //    var host2 = metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
        //    Assert.NotNull(host2);
        //    Assert.AreEqual("ut-dc2", host2.Datacenter);
        //    Assert.AreEqual("ut-rack2", host2.Rack);
        //    //with rpc_address = 0.0.0.0, use peer
        //    var host3 = metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
        //    Assert.NotNull(host3);
        //    Assert.AreEqual("ut-dc3", host3.Datacenter);
        //    Assert.AreEqual("ut-rack3", host3.Rack);
        //    Assert.AreEqual(Version.Parse("2.1.5"), host3.CassandraVersion);
        //}

        //[Test]
        //public void UpdatePeersInfoWithNullRpcIgnores()
        //{
        //    var config = new Configuration();
        //    var metadata = new Metadata(config);
        //    var cc = NewInstance(Mock.Of<IInternalCluster>(), config, metadata);
        //    cc.Host = TestHelper.CreateHost("127.0.0.1");
        //    metadata.AddHost(cc.Host.Address);
        //    var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
        //    {
        //        new Dictionary<string, object>{{"rpc_address", null}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1"}}
        //    });
        //    cc.UpdatePeersInfo(rows, cc.Host);
        //    //Only local host present
        //    Assert.AreEqual(1, metadata.AllHosts().Count);
        //}

        //[Test]
        //public void UpdatePeersInfoUsesAddressTranslator()
        //{
        //    var invokedEndPoints = new List<IPEndPoint>();
        //    var translatorMock = new Mock<IAddressTranslator>(MockBehavior.Strict);
        //    translatorMock
        //        .Setup(t => t.Translate(It.IsAny<IPEndPoint>()))
        //        .Callback<IPEndPoint>(invokedEndPoints.Add)
        //        .Returns<IPEndPoint>(e => e);
        //    const int portNumber = 9999;
        //    var metadata = new Metadata(new Configuration());
        //    var config =
        //        new TestConfigurationBuilder
        //        {
        //            ProtocolOptions = new ProtocolOptions(portNumber),
        //            AddressTranslator = translatorMock.Object,
        //            StartupOptionsFactory = Mock.Of<IStartupOptionsFactory>()
        //        }.Build();
        //    var cc = NewInstance(Mock.Of<IInternalCluster>(), config, metadata);
        //    cc.Host = TestHelper.CreateHost("127.0.0.1");
        //    metadata.AddHost(cc.Host.Address);
        //    var hostAddress2 = IPAddress.Parse("127.0.0.2");
        //    var hostAddress3 = IPAddress.Parse("127.0.0.3");
        //    var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
        //    {
        //        new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}},
        //        new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}}
        //    });
        //    cc.UpdatePeersInfo(rows, cc.Host);
        //    Assert.AreEqual(3, metadata.AllHosts().Count);
        //    Assert.AreEqual(2, invokedEndPoints.Count);
        //    Assert.AreEqual(hostAddress2, invokedEndPoints[0].Address);
        //    Assert.AreEqual(portNumber, invokedEndPoints[0].Port);
        //    Assert.AreEqual(hostAddress3, invokedEndPoints[1].Address);
        //    Assert.AreEqual(portNumber, invokedEndPoints[1].Port);
        //}
        
        //[Test]
        //public void ShouldNotAttemptDownOrIgnoredHosts()
        //{
        //    var config = new TestConfigurationBuilder()
        //    {
        //        SocketOptions = new SocketOptions().SetConnectTimeoutMillis(100).SetReadTimeoutMillis(100),
        //        Policies = new Cassandra.Policies(
        //            new ClusterUnitTests.FakeHostDistanceLbp(new Dictionary<string, HostDistance>
        //            {
        //                {"127.0.0.1", HostDistance.Local},
        //                {"127.0.0.2", HostDistance.Local},
        //                {"127.0.0.3", HostDistance.Ignored},
        //                {"127.0.0.4", HostDistance.Local}
        //            }),
        //            new ConstantReconnectionPolicy(1000),
        //            new DefaultRetryPolicy())
        //    }.Build();
        //    var cluster = Mock.Of<IInternalCluster>();
        //    var metadata = new Metadata(config);
        //    using (var cc = NewInstance(cluster, config, metadata))
        //    {
        //        cc.Host = TestHelper.CreateHost("127.0.0.1");
        //        metadata.AddHost(cc.Host.Address);
        //        var hostAddress2 = IPAddress.Parse("127.0.0.2");
        //        var hostAddress3 = IPAddress.Parse("127.0.0.3");
        //        var hostAddress4 = IPAddress.Parse("127.0.0.4");
        //        var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
        //        {
        //            new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
        //            new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}},
        //            new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress4}, { "data_center", "ut-dc3" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}}
        //        });
        //        cc.UpdatePeersInfo(rows, cc.Host);
        //        Assert.AreEqual(4, metadata.AllHosts().Count);
        //        var host2 = metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
        //        Assert.NotNull(host2);
        //        host2.SetDown();
        //        var host3 = metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
        //        Assert.NotNull(host3);

        //        Mock.Get(cluster)
        //            .Setup(c => c.RetrieveAndSetDistance(It.IsAny<Host>()))
        //            .Returns<Host>(h => config.Policies.LoadBalancingPolicy.Distance(h));
        //        Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(() => metadata.AllHosts());
        //        config.Policies.LoadBalancingPolicy.Initialize(cluster);

        //        var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => cc.Reconnect());
        //        CollectionAssert.AreEquivalent(new[] { "127.0.0.1", "127.0.0.4" }, ex.Errors.Keys.Select(e => e.Address.ToString()));
        //    }
        //}

        //[Test]
        //[TestCase(true)]
        //[TestCase(false)]
        //public void Should_ResolveContactPointsAndAttemptEveryOne_When_ContactPointResolutionReturnsMultiple(bool keepContactPointsUnresolved)
        //{
        //    var target = Create(keepContactPointsUnresolved);

        //    Assert.ThrowsAsync<NoHostAvailableException>(() => target.InitAsync());

        //    if (keepContactPointsUnresolved)
        //    {
        //        Assert.AreEqual(0, _cp1.Calls.Count(b => !b));
        //        Assert.AreEqual(0, _cp2.Calls.Count(b => !b));
        //        Assert.AreEqual(0, _localhost.Calls.Count(b => !b));
        //    }
        //    else
        //    {
        //        Assert.AreEqual(1, _cp1.Calls.Count(b => !b));
        //        Assert.AreEqual(1, _cp2.Calls.Count(b => !b));
        //        Assert.AreEqual(1, _localhost.Calls.Count(b => !b));
        //    }

        //    Assert.AreEqual(1, _cp1.Calls.Count(b => b));
        //    Assert.AreEqual(1, _cp2.Calls.Count(b => b));
        //    Assert.AreEqual(1, _localhost.Calls.Count(b => b));
        //    Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint1].Count);
        //    Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint2].Count);
        //}

        //private IControlConnection Create(bool keepContactPointsUnresolved)
        //{
        //    _connectionFactory = new FakeConnectionFactory();
        //    var config = new TestConfigurationBuilder
        //    {
        //        ConnectionFactory = _connectionFactory,
        //        KeepContactPointsUnresolved = keepContactPointsUnresolved
        //    }.Build();
        //    _cp1 = new TestContactPoint(new List<IConnectionEndPoint>
        //    {
        //        new ConnectionEndPoint(_endpoint1, config.ServerNameResolver, _cp1)
        //    });
        //    _cp2 = new TestContactPoint(new List<IConnectionEndPoint>
        //    {
        //        new ConnectionEndPoint(_endpoint2, config.ServerNameResolver, _cp2)
        //    });
        //    _localhost = new TestContactPoint(new List<IConnectionEndPoint>
        //    {
        //        new ConnectionEndPoint(_endpoint1, config.ServerNameResolver, _localhost),
        //        new ConnectionEndPoint(_endpoint2, config.ServerNameResolver, _localhost)
        //    });
        //    return new ControlConnection(
        //        Mock.Of<IInternalCluster>(),
        //        new ProtocolEventDebouncer(
        //            new FakeTimerFactory(), TimeSpan.Zero, TimeSpan.Zero), 
        //        ProtocolVersion.V3, 
        //        config, 
        //        new Metadata(config), 
        //        new List<IContactPoint>
        //        {
        //            _cp1,
        //            _cp2,
        //            _localhost
        //        });
        //}

        private class TestContactPoint : IContactPoint
        {
            public ConcurrentQueue<bool> Calls { get; } = new ConcurrentQueue<bool>();

            private readonly IEnumerable<IConnectionEndPoint> _endPoints;

            public TestContactPoint(IEnumerable<IConnectionEndPoint> endPoints)
            {
                _endPoints = endPoints;
            }

            public bool Equals(IContactPoint other)
            {
                return Equals((object) other);
            }

            public override bool Equals(object obj)
            {
                return object.ReferenceEquals(this, obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((_endPoints != null ? _endPoints.GetHashCode() : 0) * 397) ^ (Calls != null ? Calls.GetHashCode() : 0);
                }
            }

            public bool CanBeResolved => true;

            public string StringRepresentation => "123";

            public Task<IEnumerable<IConnectionEndPoint>> GetConnectionEndPointsAsync(bool refreshCache)
            {
                Calls.Enqueue(refreshCache);
                return Task.FromResult(_endPoints);
            }
        }
    }
}