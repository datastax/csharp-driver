using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tasks;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
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
        public void Cluster_Init_Keyspace_Race_Test()
        {
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace
                                        .WithDefaultKeyspace("system")
                                        //lots of connections per host
                                        .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 30))
                                        .WithQueryTimeout(45000)
                                        .Build())
            {
                var session = cluster.Connect();
                // Try to be force a race condition
                Parallel.For(0, 200, _ => session.Execute(new SimpleStatement("SELECT * FROM local")));
                var actions = new Task[200];
                for (var i = 0; i < actions.Length; i++)
                {
                    actions[i] = session.ExecuteAsync(new SimpleStatement("SELECT * FROM local"));
                }
                Task.WaitAll(actions);
                Assert.That(actions.Count(a => a.Exception != null), Is.EqualTo(0));
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
