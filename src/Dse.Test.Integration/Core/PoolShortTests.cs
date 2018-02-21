//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dse.Test.Integration.Policies.Util;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using NUnit.Framework;
using Dse.Test.Unit;

namespace Dse.Test.Integration.Core
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
    }
}