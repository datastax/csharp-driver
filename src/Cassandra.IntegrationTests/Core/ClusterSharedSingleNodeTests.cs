namespace Cassandra.IntegrationTests.Core
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    using Cassandra.IntegrationTests.TestClusterManagement;
    using Cassandra.Tasks;

    using NUnit.Framework;

    [Category("short")]
    public class ClusterSharedSingleNodeTests : SharedClusterTest
    {
        [Test]
        public void Cluster_Should_Ignore_IpV6_Addresses_For_Not_Valid_Hosts()
        {
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(IPAddress.Parse("::1"))
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                Assert.DoesNotThrow(() =>
                {
                    var session = cluster.Connect();
                    session.Execute("select * from system.local");
                });
            }
        }

        [Test]
        public void Should_Try_To_Resolve_And_Continue_With_The_Next_Contact_Point_If_It_Fails()
        {
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint("not-a-host")
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                session.Execute("select * from system.local");
                Assert.That(cluster.AllHosts().Count, Is.EqualTo(1));
            }
        }

        [Test]
        public async Task Cluster_Init_Keyspace_Race_Test()
        {
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace
                                        .WithDefaultKeyspace("system")
                                        //lots of connections per host
                                        .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 30))
                                        .Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var actionsBefore = new List<Task>();
                var actionsAfter = new List<Task>();

                // Try to force a race condition
                for (var i = 0; i < 2000; i++)
                {
                    actionsBefore.Add(session.ExecuteAsync(new SimpleStatement("SELECT * FROM local")));
                }

                await Task.WhenAll(actionsBefore.ToArray()).ConfigureAwait(false);
                Assert.True(actionsBefore.All(a => a.IsCompleted));
                Assert.False(actionsBefore.Any(a => a.IsFaulted)); ;

                for (var i = 0; i < 200; i++)
                {
                    actionsAfter.Add(session.ExecuteAsync(new SimpleStatement("SELECT * FROM local")));
                }

                await Task.WhenAll(actionsAfter.ToArray()).ConfigureAwait(false);
                Assert.True(actionsAfter.All(a => a.IsCompleted));
                Assert.False(actionsAfter.Any(a => a.IsFaulted));
            }
        }

        [Test]
        public void Cluster_Connect_With_Wrong_Keyspace_Name_Test()
        {
            var cluster = Cluster.Builder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 //using a keyspace that does not exists
                                 .WithDefaultKeyspace("MY_WRONG_KEYSPACE")
                                 .Build();

            Assert.Throws<InvalidQueryException>(() => cluster.Connect());
            Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES_NOT_EXIST"));
        }

        [Test]
        public void Cluster_Should_Resolve_Names()
        {
            IPAddress[] addressList = null;
            try
            {
                var hostEntry = TaskHelper.WaitToComplete(Dns.GetHostEntryAsync("localhost"));
                addressList = hostEntry.AddressList;
            }
            catch
            {
                Assert.Ignore("Test uses localhost and host name could not be resolved");
            }
            var contactPoint = IPAddress.Parse(TestClusterManager.IpPrefix + "1");
            if (!addressList.Contains(contactPoint))
            {
                Assert.Ignore("Test uses localhost but contact point is not localhost");
            }
            var cluster = Cluster.Builder()
                                 .AddContactPoint("localhost")
                                 .Build();
            cluster.Connect("system");
            Assert.AreEqual(contactPoint, cluster.AllHosts().First().Address.Address);
        }
    }
}