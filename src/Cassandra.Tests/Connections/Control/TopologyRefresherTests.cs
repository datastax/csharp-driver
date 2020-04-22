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
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.Tests.Connections.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections.Control
{
    [TestFixture]
    public class TopologyRefresherTests
    {
        private const string LocalQuery = "SELECT * FROM system.local WHERE key='local'";
        private const string PeersQuery = "SELECT * FROM system.peers";

        private Metadata _metadata;

        private ISerializer _serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();

        private FakeMetadataRequestHandler CreateFakeMetadataRequestHandler(
            IRow localRow = null,
            IEnumerable<IRow> peersRows = null)
        {
            var row = localRow ?? TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, 
                { "data_center", "ut-dc" }, 
                { "rack", "ut-rack" }, 
                {"tokens", null}, 
                {"release_version", "2.2.1-SNAPSHOT"},
                {"partitioner", "Murmur3Partitioner" }
            });
            var peerRows = peersRows ?? TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("127.0.0.2")}, {"peer", null}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}},
            });
            return new FakeMetadataRequestHandler(new Dictionary<string, IEnumerable<IRow>>
            {
                { TopologyRefresherTests.LocalQuery, new List<IRow> { row } },
                { TopologyRefresherTests.PeersQuery, peerRows }
            });
        }

        private TopologyRefresher CreateTopologyRefresher(
            IRow localRow = null,
            IEnumerable<IRow> peersRows = null)
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler(localRow, peersRows);
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            var metadata = new Metadata(config);
            _metadata = metadata;
            return new TopologyRefresher(metadata, config);
        }

        [Test]
        public void Should_SendSystemLocalAndPeersQueries()
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler();
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            _metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(_metadata, config);
            var connection = Mock.Of<IConnection>();

            var _ = topologyRefresher.RefreshNodeListAsync(new FakeConnectionEndPoint("127.0.0.1", 9042), connection, _serializer);

            Assert.AreEqual(TopologyRefresherTests.LocalQuery, fakeRequestHandler.Requests.First().CqlQuery);
            Assert.AreEqual(TopologyRefresherTests.PeersQuery, fakeRequestHandler.Requests.Last().CqlQuery);
        }

        [Test]
        public async Task Should_SetClusterName()
        {
            var topologyRefresher = CreateTopologyRefresher();
            var connection = Mock.Of<IConnection>();

            await topologyRefresher.RefreshNodeListAsync(
                new FakeConnectionEndPoint("127.0.0.1", 9042), connection, _serializer).ConfigureAwait(false);

            Assert.AreEqual("ut-cluster", _metadata.ClusterName);
        }

        [Test]
        public async Task Should_UpdateHostsCollection()
        {
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}}
            });
            var topologyRefresher = CreateTopologyRefresher(peersRows: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.AreEqual(3, _metadata.AllHosts().Count);
            //using rpc_address
            var host2 = _metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
            Assert.NotNull(host2);
            Assert.AreEqual("ut-dc2", host2.Datacenter);
            Assert.AreEqual("ut-rack2", host2.Rack);
            //with rpc_address = 0.0.0.0, use peer
            var host3 = _metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
            Assert.NotNull(host3);
            Assert.AreEqual("ut-dc3", host3.Datacenter);
            Assert.AreEqual("ut-rack3", host3.Rack);
            Assert.AreEqual(Version.Parse("2.1.5"), host3.CassandraVersion);
        }

        [Test]
        public async Task Should_IgnoreNullRpcAddress()
        {
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", null}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1"}}
            });
            var topologyRefresher = CreateTopologyRefresher(peersRows: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            //Only local host present
            Assert.AreEqual(1, _metadata.AllHosts().Count);
        }

        [Test]
        public async Task UpdatePeersInfoUsesAddressTranslator()
        {
            var invokedEndPoints = new List<IPEndPoint>();
            var translatorMock = new Mock<IAddressTranslator>(MockBehavior.Strict);
            translatorMock
                .Setup(t => t.Translate(It.IsAny<IPEndPoint>()))
                .Callback<IPEndPoint>(invokedEndPoints.Add)
                .Returns<IPEndPoint>(e => e);
            const int portNumber = 9999;
            var metadata = new Metadata(new Configuration());
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}}
            });
            var requestHandler = CreateFakeMetadataRequestHandler(peersRows: rows);
            var config =
                new TestConfigurationBuilder
                {
                    ProtocolOptions = new ProtocolOptions(portNumber),
                    AddressTranslator = translatorMock.Object,
                    StartupOptionsFactory = Mock.Of<IStartupOptionsFactory>(),
                    MetadataRequestHandler = requestHandler
                }.Build();
            var topologyRefresher = new TopologyRefresher(metadata, config);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.AreEqual(3, metadata.AllHosts().Count);
            Assert.AreEqual(2, invokedEndPoints.Count);
            Assert.AreEqual(hostAddress2, invokedEndPoints[0].Address);
            Assert.AreEqual(portNumber, invokedEndPoints[0].Port);
            Assert.AreEqual(hostAddress3, invokedEndPoints[1].Address);
            Assert.AreEqual(portNumber, invokedEndPoints[1].Port);
        }
    }
}