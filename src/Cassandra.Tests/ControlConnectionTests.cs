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

        [Test]
        public void UpdateLocalNodeInfoModifiesHost()
        {
            var rp = new ConstantReconnectionPolicy(1000);
            var metadata = new Metadata(rp);
            var cc = new ControlConnection(Mock.Of<ICluster>(), metadata);
            cc.Host = new Host(IPAddress.Parse("127.0.0.1"), rp);
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, { "data_center", "ut-dc" }, { "rack", "ut-rack" }, {"tokens", null}
            });
            cc.UpdateLocalInfo(row);
            Assert.AreEqual("ut-cluster", metadata.ClusterName);
            Assert.AreEqual("ut-dc", cc.Host.Datacenter);
            Assert.AreEqual("ut-rack", cc.Host.Rack);
        }

        [Test]
        public void UpdatePeersInfoModifiesPool()
        {
            var rp = new ConstantReconnectionPolicy(1000);
            var metadata = new Metadata(rp);
            var cc = new ControlConnection(Mock.Of<ICluster>(), metadata);
            cc.Host = new Host(IPAddress.Parse("127.0.0.1"), rp);
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
            //using rpc_address
            var host2 = metadata.GetHost(hostAddress2);
            Assert.NotNull(host2);
            Assert.AreEqual("ut-dc2", host2.Datacenter);
            Assert.AreEqual("ut-rack2", host2.Rack);
            //with rpc_address = 0.0.0.0, use peer
            var host3 = metadata.GetHost(hostAddress3);
            Assert.NotNull(host3);
            Assert.AreEqual("ut-dc3", host3.Datacenter);
            Assert.AreEqual("ut-rack3", host3.Rack);
        }

        [Test]
        public void UpdatePeersInfoWithNullRpcIgnores()
        {
            var rp = new ConstantReconnectionPolicy(1000);
            var metadata = new Metadata(rp);
            var cc = new ControlConnection(Mock.Of<ICluster>(), metadata);
            cc.Host = new Host(IPAddress.Parse("127.0.0.1"), rp);
            metadata.AddHost(cc.Host.Address);
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", null}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack" }, {"tokens", null}}
            });
            cc.UpdatePeersInfo(rows);
            //Only local host present
            Assert.AreEqual(1, metadata.AllHosts().Count);
        }
    }
}
