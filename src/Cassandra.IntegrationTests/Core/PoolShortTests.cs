using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class PoolShortTests : TestGlobals
    {
        [TearDown]
        public void OnTearDown()
        {
            TestClusterManager.TryRemove();
        }

        [Test, TestTimeout(1000 * 60 * 4), TestCase(false), TestCase(true)]
        public void StopForce_With_Inflight_Requests(bool useStreamMode)
        {
            var testCluster = TestClusterManager.CreateNew(2);
            const int connectionLength = 4;
            var builder = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .WithPoolingOptions(new PoolingOptions()
                    .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                    .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                    .SetHeartBeatInterval(0))
                .WithRetryPolicy(AlwaysIgnoreRetryPolicy.Instance)
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0).SetStreamMode(useStreamMode))
                .WithLoadBalancingPolicy(new RoundRobinPolicy());
            using (var cluster = builder.Build())
            {
                var session = (Session)cluster.Connect();
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, "ks1", 2));
                session.Execute("CREATE TABLE ks1.table1 (id1 int, id2 int, PRIMARY KEY (id1, id2))");
                var ps = session.Prepare("INSERT INTO ks1.table1 (id1, id2) VALUES (?, ?)");
                var t = ExecuteMultiple(testCluster, session, ps, false, 1, 100);
                t.Wait();
                Assert.AreEqual(2, t.Result.Length, "The 2 hosts must have been used");
                // Wait for all connections to be opened
                Thread.Sleep(1000);
                var hosts = cluster.AllHosts().ToArray();
                TestHelper.WaitUntil(() =>
                    hosts.Sum(h => session
                        .GetOrCreateConnectionPool(h, HostDistance.Local)
                        .OpenConnections
                    ) == hosts.Length * connectionLength);
                Assert.AreEqual(
                    hosts.Length * connectionLength, 
                    hosts.Sum(h => session.GetOrCreateConnectionPool(h, HostDistance.Local).OpenConnections));
                ExecuteMultiple(testCluster, session, ps, true, 8000, 200000).Wait();
            }
        }

        private Task<string[]> ExecuteMultiple(ITestCluster testCluster, Session session, PreparedStatement ps, bool stopNode, int maxConcurrency, int repeatLength)
        {
            var hosts = new ConcurrentDictionary<string, bool>();
            var tcs = new TaskCompletionSource<string[]>();
            var receivedCounter = 0L;
            var sendCounter = 0L;
            var currentlySentCounter = 0L;
            var stopMark = repeatLength / 8L;
            Action sendNew = null;
            sendNew = () =>
            {
                var sent = Interlocked.Increment(ref sendCounter);
                if (sent > repeatLength)
                {
                    return;
                }
                Interlocked.Increment(ref currentlySentCounter);
                var statement = ps.Bind(DateTime.Now.Millisecond, (int)sent);
                var executeTask = session.ExecuteAsync(statement);
                executeTask.ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        tcs.TrySetException(t.Exception.InnerException);
                        return;
                    }
                    hosts.AddOrUpdate(t.Result.Info.QueriedHost.ToString(), true, (k, v) => v);
                    var received = Interlocked.Increment(ref receivedCounter);
                    if (stopNode && received == stopMark)
                    {

                        Task.Factory.StartNew(() =>
                        {
                            testCluster.StopForce(2);
                        }, TaskCreationOptions.LongRunning);
                    }
                    if (received == repeatLength)
                    {
                        // Mark this as finished
                        tcs.TrySetResult(hosts.Keys.ToArray());
                        return;
                    }
                    sendNew();
                }, TaskContinuationOptions.ExecuteSynchronously);
            };

            for (var i = 0; i < maxConcurrency; i++)
            {
                sendNew();
            }
            return tcs.Task;
        }

        [Test]
        public void MarkHostDown_PartialPoolConnection()
        {
            var sCluster = SimulacronCluster.CreateNew(new SimulacronOptions());
            const int connectionLength = 4;
            var builder = Cluster.Builder()
                                 .AddContactPoint(sCluster.InitialContactPoint)
                                 .WithPoolingOptions(new PoolingOptions()
                                     .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetHeartBeatInterval(2000))
                                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(long.MaxValue));
            using (var cluster = builder.Build())
            {
                var session = (Session)cluster.Connect();
                var allHosts = cluster.AllHosts();

                TestHelper.WaitUntil(() =>
                    allHosts.Sum(h => session
                        .GetOrCreateConnectionPool(h, HostDistance.Local)
                        .OpenConnections
                    ) == allHosts.Count * connectionLength);
                var h1 = allHosts.FirstOrDefault();
                var pool = session.GetOrCreateConnectionPool(h1, HostDistance.Local);
                var ports = sCluster.GetConnectedPorts();
                // 4 pool connections + the control connection
                Assert.AreEqual(5, ports.Count);
                sCluster.DisableConnectionListener().Wait();
                // Remove the first connections
                for (var i = 0; i < 3; i++)
                {
                    // Closure
                    var index = i;
                    var expectedOpenConnections = 5 - index;
                    WaitSimulatorConnections(sCluster, expectedOpenConnections);
                    ports = sCluster.GetConnectedPorts();
                    Assert.AreEqual(expectedOpenConnections, ports.Count, "Cassandra simulator contains unexpected number of connected clients");
                    sCluster.DropConnection(ports.Last().Address.ToString(), ports.Last().Port).Wait();
                    // Host pool could have between pool.OpenConnections - i and pool.OpenConnections - i - 1
                    TestHelper.WaitUntil(() => pool.OpenConnections >= 4 - index - 1 && pool.OpenConnections <= 4 - index);
                    Assert.LessOrEqual(pool.OpenConnections, 4 - index);
                    Assert.GreaterOrEqual(pool.OpenConnections, 4 - index - 1);
                    Assert.IsTrue(h1.IsUp);
                }
                WaitSimulatorConnections(sCluster, 2);
                ports = sCluster.GetConnectedPorts();
                Assert.AreEqual(2, ports.Count);
                sCluster.DropConnection(ports.Last().Address.ToString(), ports.Last().Port).Wait();
                sCluster.DropConnection(ports.First().Address.ToString(), ports.First().Port).Wait();
                TestHelper.WaitUntil(() => pool.OpenConnections == 0 && !h1.IsUp);
                Assert.IsFalse(h1.IsUp);
            }
        }

        /// <summary>
        /// Waits for the simulator to have the expected number of connections
        /// </summary>
        private static void WaitSimulatorConnections(SimulacronCluster sSimulacronCluster, int expected)
        {
            TestHelper.WaitUntil(() => sSimulacronCluster.GetConnectedPorts().Count == expected);
        }

        /// <summary>
        /// Tests that if no host is available at Cluster.Init(), it will initialize next time it is invoked
        /// </summary>
        [Test]
        public void Cluster_Initialization_Recovers_From_NoHostAvailableException()
        {
            var testCluster = TestClusterManager.CreateNew();
            testCluster.StopForce(1);
            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .Build();
            //initially it will throw as there is no node reachable
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect());

            // wait for the node to be up
            testCluster.Start(1);
            TestUtils.WaitForUp(testCluster.InitialContactPoint, 9042, 5);
            // Now the node is ready to accept connections
            var session = cluster.Connect("system");
            TestHelper.ParallelInvoke(() => session.Execute("SELECT * from local"), 20);
        }

        [Test]
        public void Connect_With_Ssl_Test()
        {
            //use ssl
            var testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions { UseSsl = true });

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithSSL(new SSLOptions().SetRemoteCertValidationCallback((a, b, c, d) => true))
                                        .Build())
            {
                Assert.DoesNotThrow(() =>
                {
                    var session = cluster.Connect();
                    TestHelper.Invoke(() => session.Execute("select * from system.local"), 10);
                });
            }
        }

        [Test]
        [TestCase(ProtocolVersion.MaxSupported, 1)]
        [TestCase(ProtocolVersion.V2, 2)]
        public void PoolingOptions_Create_Based_On_Protocol(ProtocolVersion protocolVersion, int coreConnectionLength)
        {
            var sCluster = SimulacronCluster.CreateNew(new SimulacronOptions());
            var options1 = PoolingOptions.Create(protocolVersion);
            using(var cluster = Cluster.Builder()
                                       .AddContactPoint(sCluster.InitialContactPoint)
                                       .WithPoolingOptions(options1)
                                       .Build())
            {
                var session = (Session) cluster.Connect();
                var allHosts = cluster.AllHosts();
                var host = allHosts.First();
                var pool = session.GetOrCreateConnectionPool(host, HostDistance.Local);

                TestHelper.WaitUntil(() =>
                    pool.OpenConnections == coreConnectionLength);
                var ports = sCluster.GetConnectedPorts();
                //coreConnectionLength + 1 (the control connection) 
                Assert.AreEqual(coreConnectionLength + 1, ports.Count);
            }
        }

        [Test]
        public async Task ControlConnection_Should_Reconnect_To_Up_Host()
        {
            const int connectionLength = 1;
            var builder = Cluster.Builder()
                                 .WithPoolingOptions(new PoolingOptions()
                                     .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetHeartBeatInterval(1000))
                                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L));
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = (Session)cluster.Connect();
                var allHosts = cluster.AllHosts();
                Assert.AreEqual(3, allHosts.Count);
                await TestHelper.TimesLimit(() =>
                    session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")), 100, 16);

                // 1 per hosts + control connection
                WaitSimulatorConnections(testCluster, 4);
                Assert.AreEqual(4, testCluster.GetConnectedPorts().Count);

                var ccAddress = cluster.GetControlConnection().Address;
                var simulacronNode = testCluster.GetNode(ccAddress);

                // Disable new connections to the first host
                await simulacronNode.DisableConnectionListener();

                Assert.NotNull(simulacronNode);
                var connections = simulacronNode.GetConnections();

                // Drop connections to the host that is being used by the control connection
                Assert.AreEqual(2, connections.Count);
                await testCluster.DropConnection(connections[0]);
                await testCluster.DropConnection(connections[1]);

                TestHelper.WaitUntil(() => !cluster.GetHost(ccAddress).IsUp);

                Assert.False(cluster.GetHost(ccAddress).IsUp);

                TestHelper.WaitUntil(() => !cluster.GetControlConnection().Address.Address.Equals(ccAddress.Address));

                Assert.AreNotEqual(ccAddress.Address, cluster.GetControlConnection().Address.Address);

                // Previous host is still DOWN
                Assert.False(cluster.GetHost(ccAddress).IsUp);

                // New host is UP
                ccAddress = cluster.GetControlConnection().Address;
                Assert.True(cluster.GetHost(ccAddress).IsUp);
            }
        }

        [Test]
        public async Task ControlConnection_Should_Reconnect_After_Failed_Attemps()
        {
            const int connectionLength = 1;
            var builder = Cluster.Builder()
                                 .WithPoolingOptions(new PoolingOptions()
                                     .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                                     .SetHeartBeatInterval(1000))
                                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L));
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = (Session)cluster.Connect();
                var allHosts = cluster.AllHosts();
                Assert.AreEqual(3, allHosts.Count);
                await TestHelper.TimesLimit(() =>
                    session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")), 100, 16);

                var serverConnections = testCluster.GetConnectedPorts();
                // 1 per hosts + control connection
                WaitSimulatorConnections(testCluster, 4);
                Assert.AreEqual(4, serverConnections.Count);

                // Disable all connections
                await testCluster.DisableConnectionListener();

                var ccAddress = cluster.GetControlConnection().Address;

                // Drop all connections to hosts
                foreach (var connection in serverConnections)
                {
                    await testCluster.DropConnection(connection);
                }

                TestHelper.WaitUntil(() => !cluster.GetHost(ccAddress).IsUp);

                // All host should be down by now
                TestHelper.WaitUntil(() => cluster.AllHosts().All(h => !h.IsUp));

                Assert.False(cluster.GetHost(ccAddress).IsUp);

                // Allow new connections to be created
                await testCluster.EnableConnectionListener();

                TestHelper.WaitUntil(() => cluster.AllHosts().All(h => h.IsUp));

                ccAddress = cluster.GetControlConnection().Address;
                Assert.True(cluster.GetHost(ccAddress).IsUp);

                // Once all connections are created, the control connection should be usable
                WaitSimulatorConnections(testCluster, 4);
                Assert.DoesNotThrowAsync(() => cluster.GetControlConnection().QueryAsync("SELECT * FROM system.local"));
            }
        }

        [Test]
        public async Task Should_Use_Next_Host_When_First_Host_Is_Busy()
        {
            const int connectionLength = 2;
            const int maxRequestsPerConnection = 100;
            var builder = Cluster.Builder()
                                 .WithPoolingOptions(
                                     PoolingOptions.Create()
                                                   .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                                                   .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                                                   .SetHeartBeatInterval(0)
                                                   .SetMaxRequestsPerConnection(maxRequestsPerConnection))
                                 .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy());
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                const string query = "SELECT * FROM simulated_ks.table1";
                testCluster.Prime(new
                {
                    when = new { query },
                    then = new { result = "success", delay_in_ms = 3000 }
                });

                var session = await cluster.ConnectAsync();
                var hosts = cluster.AllHosts().ToArray();

                // Wait until all connections to first host are created
                await TestHelper.WaitUntilAsync(() =>
                    session.GetState().GetInFlightQueries(hosts[0]) == connectionLength);

                const int overflowToNextHost = 10;
                var length = maxRequestsPerConnection * connectionLength + Environment.ProcessorCount +
                             overflowToNextHost;
                var tasks = new List<Task<RowSet>>(length);

                for (var i = 0; i < length; i++)
                {
                    tasks.Add(session.ExecuteAsync(new SimpleStatement(query)));
                }

                var results = await Task.WhenAll(tasks);

                // At least the first n (maxRequestsPerConnection * connectionLength) went to the first host
                Assert.That(results.Count(r => r.Info.QueriedHost.Equals(hosts[0].Address)),
                    Is.GreaterThanOrEqualTo(maxRequestsPerConnection * connectionLength));

                // At least the following m (overflowToNextHost) went to the second host
                Assert.That(results.Count(r => r.Info.QueriedHost.Equals(hosts[1].Address)),
                    Is.GreaterThanOrEqualTo(overflowToNextHost));
            }
        }

        [Test]
        public async Task Should_Throw_NoHostAvailableException_When_All_Host_Are_Busy()
        {
            const int connectionLength = 2;
            const int maxRequestsPerConnection = 50;
            var lbp = new TestHelper.OrderedLoadBalancingPolicy().UseRoundRobin();

            var builder = Cluster.Builder()
                                 .WithPoolingOptions(
                                     PoolingOptions.Create()
                                                   .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                                                   .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                                                   .SetHeartBeatInterval(0)
                                                   .SetMaxRequestsPerConnection(maxRequestsPerConnection))
                                 .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                                 .WithLoadBalancingPolicy(lbp);

            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                const string query = "SELECT * FROM simulated_ks.table1";
                testCluster.Prime(new
                {
                    when = new { query },
                    then = new { result = "success", delay_in_ms = 3000 }
                });

                var session = await cluster.ConnectAsync();
                var hosts = cluster.AllHosts().ToArray();

                await TestHelper.TimesLimit(() =>
                    session.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local")), 100, 16);

                // Wait until all connections to all host are created
                await TestHelper.WaitUntilAsync(() =>
                {
                    var state = session.GetState();
                    return state.GetConnectedHosts().All(h => state.GetInFlightQueries(h) == connectionLength);
                });

                lbp.UseFixedOrder();

                const int busyExceptions = 10;
                var length = maxRequestsPerConnection * connectionLength * hosts.Length + Environment.ProcessorCount +
                             busyExceptions;
                var tasks = new List<Task<Exception>>(length);

                for (var i = 0; i < length; i++)
                {
                    tasks.Add(TestHelper.EatUpException(session.ExecuteAsync(new SimpleStatement(query))));
                }

                var results = await Task.WhenAll(tasks);

                // Only successful responses or NoHostAvailableException expected
                Assert.Null(results.FirstOrDefault(e => e != null && !(e is NoHostAvailableException)));

                // At least the first n (maxRequestsPerConnection * connectionLength * hosts.length) succeeded
                Assert.That(results.Count(e => e == null),
                    Is.GreaterThanOrEqualTo(maxRequestsPerConnection * connectionLength * hosts.Length));

                // At least the following m (busyExceptions) failed
                var failed = results.Where(e => e is NoHostAvailableException).Cast<NoHostAvailableException>()
                                    .ToArray();
                Assert.That(failed, Has.Length.GreaterThanOrEqualTo(busyExceptions));

                foreach (var ex in failed)
                {
                    Assert.That(ex.Errors, Has.Count.EqualTo(hosts.Length));

                    foreach (var kv in ex.Errors)
                    {
                        Assert.IsInstanceOf<BusyPoolException>(kv.Value);
                        var busyException = (BusyPoolException) kv.Value;
                        Assert.AreEqual(kv.Key, busyException.Address);
                        Assert.That(busyException.ConnectionLength, Is.EqualTo(connectionLength));
                        Assert.That(busyException.MaxRequestsPerConnection, Is.EqualTo(maxRequestsPerConnection));
                        Assert.That(busyException.Message, Is.EqualTo(
                            $"All connections to host {busyException.Address} are busy, {maxRequestsPerConnection}" +
                            $" requests are in-flight on each {connectionLength} connection(s)"));
                    }
                }
            }
        }
    }
}