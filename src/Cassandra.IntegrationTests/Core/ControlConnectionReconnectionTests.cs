using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class ControlConnectionReconnectionTests : TestGlobals
    {
        private ControlConnection NewInstance(ITestCluster testCluster, Configuration config = null, Metadata metadata = null)
        {
            var version = (byte)Cluster.MaxProtocolVersion;
            if (config == null)
            {
                config = new Configuration();
            }
            if (metadata == null)
            {
                metadata = new Metadata(config);
                metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            }
            var cc = new ControlConnection(version, config, metadata);
            metadata.ControlConnection = cc;
            return cc;
        }

        [Test]
        public async Task Should_Schedule_Reconnections_In_The_Background()
        {
            var lbp = new RoundRobinPolicy();
            var config = new Configuration(
                new Cassandra.Policies(lbp, new ConstantReconnectionPolicy(1000), FallthroughRetryPolicy.Instance),
                new ProtocolOptions(),
                null,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config, metadata))
            {
                await cc.InitAsync();
                var host = metadata.Hosts.First();
                testCluster.Stop(1);
                host.SetDown();
                Thread.Sleep(2000);
                Assert.False(host.IsUp);
                testCluster.Start(1);
                host.BringUpIfDown();
                //Should reconnect using timer
                Thread.Sleep(5000);
                Assert.DoesNotThrow(() => cc.Query("SELECT key FROM system.local", false));
            }
            testCluster.ShutDown();
        }

        [Test]
        public async Task Should_Reconnect_Once_If_Called_Serially()
        {
            var lbp = new RoundRobinPolicy();
            var config = new Configuration(
                new Cassandra.Policies(lbp, new ConstantReconnectionPolicy(1000), FallthroughRetryPolicy.Instance),
                new ProtocolOptions(),
                null,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config))
            {
                await cc.InitAsync();
                testCluster.Stop(1);
                var t1 = cc.Reconnect(CancellationToken.None);
                var t2 = cc.Reconnect(CancellationToken.None);
                var t3 = cc.Reconnect(CancellationToken.None);
                var t4 = cc.Reconnect(CancellationToken.None);
                Assert.AreEqual(t1, t2);
                Assert.AreEqual(t1, t3);
                Assert.AreEqual(t1, t4);
                var ex = Assert.Throws<NoHostAvailableException>(() => t1.WaitToComplete());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsInstanceOf<SocketException>(ex.Errors.Values.First());
            }
            testCluster.ShutDown();
        }

        [Test]
        public async Task Should_Reconnect_After_Several_Failed_Attempts()
        {
            var lbp = new RoundRobinPolicy();
            var config = new Configuration(
                new Cassandra.Policies(lbp, new ConstantReconnectionPolicy(1000), FallthroughRetryPolicy.Instance),
                new ProtocolOptions(),
                null,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config))
            {
                await cc.InitAsync();
                testCluster.Stop(1);
                Assert.Throws<NoHostAvailableException>(() => cc.Reconnect(CancellationToken.None).WaitToComplete());
                Assert.Throws<NoHostAvailableException>(() => cc.Reconnect(CancellationToken.None).WaitToComplete());
                Assert.Throws<NoHostAvailableException>(() => cc.Reconnect(CancellationToken.None).WaitToComplete());
                Assert.Throws<NoHostAvailableException>(() => cc.Reconnect(CancellationToken.None).WaitToComplete());
                testCluster.Start(1);
                Assert.DoesNotThrow(() => cc.Reconnect(CancellationToken.None).WaitToComplete());
            }
            testCluster.ShutDown();
        }
    }
}
