using Cassandra.Connections.Control;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class ShardAwareOptionsTests : TestGlobals
    {
        private ITestCluster _realCluster;

        [TearDown]
        public void TestTearDown()
        {
            TestClusterManager.TryRemove();
            _realCluster = null;
        }

        [Test]
        public void Should_Connect_To_Shard_Aware_Cluster()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var session = cluster.Connect();
            IInternalCluster internalCluster = cluster;
            var controlConnection = (ControlConnection)internalCluster.GetControlConnection();
            Assert.IsTrue(controlConnection.IsShardAware());
        }
    }
}