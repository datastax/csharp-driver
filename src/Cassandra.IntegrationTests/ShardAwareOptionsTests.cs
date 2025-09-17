using Cassandra.Connections.Control;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using System.Linq;

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

        [TestCase(1)]
        [TestCase(4)]
        public void Should_Have_NrShards_Connections(int connectionsPerHost)
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .WithPoolingOptions(new PoolingOptions()
                              .SetCoreConnectionsPerHost(HostDistance.Local, connectionsPerHost))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var session = cluster.Connect();
            IInternalSession internalSession = (IInternalSession)session;
            var pools = internalSession.GetPools();
            foreach (var kvp in pools)
            {
                var shardCount = 2;
                var connectionsPerShard = connectionsPerHost / shardCount + (connectionsPerHost % shardCount > 0 ? 1 : 0);
                Assert.AreEqual(shardCount * connectionsPerShard, kvp.Value.OpenConnections);
                var shardGroups = kvp.Value.ConnectionsSnapshot.GroupBy(c => c.ShardID);
                Assert.IsTrue(shardGroups.All(g => g.Count() == connectionsPerShard));
            }
        }
    }
}
