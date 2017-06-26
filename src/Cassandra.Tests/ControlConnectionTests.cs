using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class ControlConnectionTests
    {
        public ControlConnectionTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        private ControlConnection NewInstance(Configuration config, Metadata metadata)
        {
            return new ControlConnection(ProtocolVersion.MaxSupported, config, metadata);
        }

        private ControlConnection NewInstance(Metadata metadata)
        {
            return NewInstance(new Configuration(), metadata);
        }

        [Test]
        public void UpdateLocalNodeInfoModifiesHost()
        {
            var metadata = new Metadata(new Configuration());
            var cc = NewInstance(metadata);
            cc.Host = TestHelper.CreateHost("127.0.0.1");
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, { "data_center", "ut-dc" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1-SNAPSHOT"}
            });
            cc.UpdateLocalInfo(row);
            Assert.AreEqual("ut-cluster", metadata.ClusterName);
            Assert.AreEqual("ut-dc", cc.Host.Datacenter);
            Assert.AreEqual("ut-rack", cc.Host.Rack);
            Assert.AreEqual(Version.Parse("2.2.1"), cc.Host.CassandraVersion);
        }

        [Test]
        public void UpdatePeersInfoModifiesPool()
        {
            var metadata = new Metadata(new Configuration());
            var cc = NewInstance(metadata);
            cc.Host = TestHelper.CreateHost("127.0.0.1");
            metadata.AddHost(cc.Host.Address);
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}}
            });
            cc.UpdatePeersInfo(rows);
            Assert.AreEqual(3, metadata.AllHosts().Count);
            //using rpc_address
            var host2 = metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
            Assert.NotNull(host2);
            Assert.AreEqual("ut-dc2", host2.Datacenter);
            Assert.AreEqual("ut-rack2", host2.Rack);
            //with rpc_address = 0.0.0.0, use peer
            var host3 = metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
            Assert.NotNull(host3);
            Assert.AreEqual("ut-dc3", host3.Datacenter);
            Assert.AreEqual("ut-rack3", host3.Rack);
            Assert.AreEqual(Version.Parse("2.1.5"), host3.CassandraVersion);
        }

        [Test]
        public void UpdatePeersInfoWithNullRpcIgnores()
        {
            var metadata = new Metadata(new Configuration());
            var cc = NewInstance(metadata);
            cc.Host = TestHelper.CreateHost("127.0.0.1");
            metadata.AddHost(cc.Host.Address);
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", null}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1"}}
            });
            cc.UpdatePeersInfo(rows);
            //Only local host present
            Assert.AreEqual(1, metadata.AllHosts().Count);
        }

        [Test]
        public void UpdatePeersInfoUsesAddressTranslator()
        {
            var invokedEndPoints = new List<IPEndPoint>();
            var translatorMock = new Mock<IAddressTranslator>(MockBehavior.Strict);
            translatorMock
                .Setup(t => t.Translate(It.IsAny<IPEndPoint>()))
                .Callback<IPEndPoint>(invokedEndPoints.Add)
                .Returns<IPEndPoint>(e => e);
            const int portNumber = 9999;
            var metadata = new Metadata(new Configuration());
            var config = new Configuration(Policies.DefaultPolicies,
                 new ProtocolOptions(portNumber),
                 null,
                 new SocketOptions(),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 translatorMock.Object);
            var cc = NewInstance(config, metadata);
            cc.Host = TestHelper.CreateHost("127.0.0.1");
            metadata.AddHost(cc.Host.Address);
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}}
            });
            cc.UpdatePeersInfo(rows);
            Assert.AreEqual(3, metadata.AllHosts().Count);
            Assert.AreEqual(2, invokedEndPoints.Count);
            Assert.AreEqual(hostAddress2, invokedEndPoints[0].Address);
            Assert.AreEqual(portNumber, invokedEndPoints[0].Port);
            Assert.AreEqual(hostAddress3, invokedEndPoints[1].Address);
            Assert.AreEqual(portNumber, invokedEndPoints[1].Port);
        }
    }
}
