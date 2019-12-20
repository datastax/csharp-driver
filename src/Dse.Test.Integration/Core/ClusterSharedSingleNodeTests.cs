using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Dse.Tasks;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;

using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    public class ClusterSharedSingleNodeTests : SimulacronTest
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"MY_WRONG_KEYSPACE\"").ThenServerError(ServerError.Invalid, "msg"));
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"ANOTHER_THAT_DOES_NOT_EXIST\"").ThenServerError(ServerError.Invalid, "msg"));

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace that does not exists
                                        .WithDefaultKeyspace("MY_WRONG_KEYSPACE")
                                        .Build())
            {
                Assert.Throws<InvalidQueryException>(() => cluster.Connect());
                Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES_NOT_EXIST"));
            }
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

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint("localhost")
                                        .Build())
            {
                try
                {
                    cluster.Connect("system");
                    Assert.IsTrue(
                        cluster.AllHosts().Any(h => addressList.Contains(h.Address.Address)),
                        string.Join(";", cluster.AllHosts().Select(h => h.Address.ToString())) + " | " + TestCluster.InitialContactPoint.Address);
                }
                catch (NoHostAvailableException ex)
                {
                    Assert.IsTrue(ex.Errors.Keys.Select(k => k.Address).OrderBy(a => a.ToString()).SequenceEqual(addressList.OrderBy(a => a.ToString())));
                }
            }
        }
    }
}