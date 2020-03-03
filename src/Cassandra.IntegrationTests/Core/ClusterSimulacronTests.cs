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

using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    public class ClusterSimulacronTests : SimulacronTest
    {
        public ClusterSimulacronTests() : base(false, new SimulacronOptions() {Nodes = "3"})
        {
            
        }

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
                Assert.That(cluster.AllHosts().Count, Is.EqualTo(3));
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

        [Test]
        public void RepeatedClusterConnectCallsAfterTimeoutErrorThrowCachedInitErrorException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            var timeoutMessage = "Cluster initialization was aborted after timing out. This mechanism is put in place to" +
                                 " avoid blocking the calling thread forever. This usually caused by a networking issue" +
                                 " between the client driver instance and the cluster. You can increase this timeout via " +
                                 "the SocketOptions.ConnectTimeoutMillis config setting. This can also be related to deadlocks " +
                                 "caused by mixing synchronous and asynchronous code.";
            var cachedError = "An error occured during the initialization of the cluster instance. Further initialization attempts " +
                              "for this cluster instance will never succeed and will return this exception instead. The InnerException property holds " +
                              "a reference to the exception that originally caused the initialization error.";
            using (var cluster =
                Cluster.Builder()
                       .AddContactPoints(TestCluster.ContactPoints)
                       .WithSocketOptions(
                           new SocketOptions()
                               .SetConnectTimeoutMillis(500)
                               .SetMetadataAbortTimeout(500))
                       .Build())
            {
                var ex = Assert.Throws<TimeoutException>(() => cluster.Connect());
                Assert.AreEqual(timeoutMessage, ex.Message);
                var ex2 = Assert.Throws<CachedInitErrorException>(() => cluster.Connect("sample_ks"));
                Assert.AreEqual(cachedError, ex2.Message);
                Assert.AreEqual(typeof(TimeoutException), ex2.InnerException.GetType());
                Assert.AreEqual(timeoutMessage, ex2.InnerException.Message);
            }
        }

        [Test]
        public void RepeatedClusterConnectCallsAfterTimeoutErrorEventuallyThrowNoHostException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            using (var cluster =
                Cluster.Builder()
                       .AddContactPoints(TestCluster.ContactPoints)
                       .WithSocketOptions(
                           new SocketOptions()
                               .SetConnectTimeoutMillis(500)
                               .SetMetadataAbortTimeout(500))
                       .Build())
            {
                Assert.Throws<TimeoutException>(() => cluster.Connect());
                TestHelper.RetryAssert(
                    () =>
                    {
                        var ex2 = Assert.Throws<CachedInitErrorException>(() => cluster.Connect("sample_ks"));
                        Assert.AreEqual(typeof(NoHostAvailableException), ex2.InnerException.GetType());
                    },
                    1000,
                    30);
            }
        }

        [Test]
        public void RepeatedClusterConnectCallsAfterNoHostErrorDontThrowCachedInitErrorException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            using (var cluster =
                Cluster.Builder()
                       .AddContactPoints(TestCluster.ContactPoints)
                       .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(1).SetReadTimeoutMillis(1))
                       .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                var ex2 = Assert.Throws<NoHostAvailableException>(() => cluster.Connect("sample_ks"));
                Assert.AreNotSame(ex, ex2);
            }
        }
    }
}