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
        private const int InitTimeout = 2000;

        private ControlConnection NewInstance(ITestCluster testCluster, Configuration config = null, Metadata metadata = null)
        {
            var version = GetProtocolVersion();
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
        public void Should_Schedule_Reconnections_In_The_Background()
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config, metadata))
            {
                cc.Init().Wait(InitTimeout);
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
        public async Task Should_Issue_Reconnect_Once()
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config))
            {
                await cc.Init();
                testCluster.Stop(1);
                const int lengthPerProcessor = 20;
                var arr = new Task[Environment.ProcessorCount * lengthPerProcessor];
                Parallel.For(0, Environment.ProcessorCount, j =>
                {
                    for (var i = 0; i < lengthPerProcessor; i++)
                    {
                        arr[j * lengthPerProcessor + i] = cc.Reconnect();
                    }
                });
                // When any of the tasks completes, all the task should have completed
                await Task.WhenAny(arr);
                await Task.Delay(20);

                var notFaultedTaskStatus = arr.Where(t => !t.IsFaulted)
                                              .Select(t => (TaskStatus?)t.Status).FirstOrDefault();
                Assert.Null(notFaultedTaskStatus, "Expected only faulted tasks");
                var ex = Assert.Throws<NoHostAvailableException>(() => TaskHelper.WaitToComplete(arr[0]));
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsInstanceOf<SocketException>(ex.Errors.Values.First());
            }
            testCluster.ShutDown();
        }

        [Test]
        public void Should_Reconnect_After_Several_Failed_Attempts()
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            var metadata = new Metadata(config);
            metadata.AddHost(new IPEndPoint(IPAddress.Parse(testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.AllHosts()).Returns(() => metadata.Hosts.ToCollection());
            lbp.Initialize(clusterMock.Object);
            using (var cc = NewInstance(testCluster, config))
            {
                cc.Init().Wait(InitTimeout);
                testCluster.Stop(1);
                Assert.Throws<NoHostAvailableException>(() => TaskHelper.WaitToComplete(cc.Reconnect()));
                Assert.Throws<NoHostAvailableException>(() => TaskHelper.WaitToComplete(cc.Reconnect()));
                Assert.Throws<NoHostAvailableException>(() => TaskHelper.WaitToComplete(cc.Reconnect()));
                Assert.Throws<NoHostAvailableException>(() => TaskHelper.WaitToComplete(cc.Reconnect()));
                testCluster.Start(1);
                Assert.DoesNotThrow(() => TaskHelper.WaitToComplete(cc.Reconnect()));
            }
            testCluster.ShutDown();
        }
    }
}