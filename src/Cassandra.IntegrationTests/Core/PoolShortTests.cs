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
using Cassandra.SessionManagement;
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
                var session = (IInternalSession)cluster.Connect();
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

        private Task<string[]> ExecuteMultiple(ITestCluster testCluster, IInternalSession session, PreparedStatement ps, bool stopNode, int maxConcurrency, int repeatLength)
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
                var session = (IInternalSession)cluster.Connect();
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
                var session = (IInternalSession) cluster.Connect();
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
        public async Task Should_Create_Core_Connections_To_Hosts_In_Local_Dc_When_Warmup_Is_Enabled()
        {
            const int nodeLength = 4;
            var poolingOptions = PoolingOptions.Create().SetCoreConnectionsPerHost(HostDistance.Local, 5);

            // Use multiple DCs: 4 nodes in first DC and 3 nodes in second DC
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = $"{nodeLength},3"}))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithPoolingOptions(poolingOptions).Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var state = session.GetState();
                var hosts = state.GetConnectedHosts();

                Assert.AreEqual(nodeLength, hosts.Count);
                foreach (var host in hosts)
                {
                    Assert.AreEqual(poolingOptions.GetCoreConnectionsPerHost(HostDistance.Local),
                                    state.GetOpenConnections(host));
                }
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
            using (var testCluster = SimulacronCluster.CreateNew(3))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = (Session)cluster.Connect();
                var allHosts = cluster.AllHosts();
                Assert.AreEqual(3, allHosts.Count);
                await TestHelper.TimesLimit(() =>
                    session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")), 100, 16).ConfigureAwait(false);

                // 1 per hosts + control connection
                WaitSimulatorConnections(testCluster, 4);
                Assert.AreEqual(4, testCluster.GetConnectedPorts().Count);

                var ccAddress = cluster.InternalRef.GetControlConnection().Address;
                Assert.NotNull(ccAddress);
                var simulacronNode = testCluster.GetNode(ccAddress);

                // Disable new connections to the first host
                await simulacronNode.Stop().ConfigureAwait(false);

                TestHelper.WaitUntil(() => !cluster.GetHost(ccAddress).IsUp);

                Assert.False(cluster.GetHost(ccAddress).IsUp);

                TestHelper.WaitUntil(() => !cluster.InternalRef.GetControlConnection().Address.Address.Equals(ccAddress.Address));
                Assert.NotNull(cluster.InternalRef.GetControlConnection().Address);
                Assert.AreNotEqual(ccAddress.Address, cluster.InternalRef.GetControlConnection().Address.Address);

                // Previous host is still DOWN
                Assert.False(cluster.GetHost(ccAddress).IsUp);

                // New host is UP
                ccAddress = cluster.InternalRef.GetControlConnection().Address;
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
                    session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")), 100, 16).ConfigureAwait(false);

                var serverConnections = testCluster.GetConnectedPorts();
                // 1 per hosts + control connection
                WaitSimulatorConnections(testCluster, 4);
                Assert.AreEqual(4, serverConnections.Count);

                // Disable all connections
                await testCluster.DisableConnectionListener().ConfigureAwait(false);

                var ccAddress = cluster.InternalRef.GetControlConnection().Address;

                // Drop all connections to hosts
                foreach (var connection in serverConnections)
                {
                    await testCluster.DropConnection(connection).ConfigureAwait(false);
                }

                TestHelper.WaitUntil(() => !cluster.GetHost(ccAddress).IsUp);

                // All host should be down by now
                TestHelper.WaitUntil(() => cluster.AllHosts().All(h => !h.IsUp));

                Assert.False(cluster.GetHost(ccAddress).IsUp);

                // Allow new connections to be created
                await testCluster.EnableConnectionListener().ConfigureAwait(false);

                TestHelper.WaitUntil(() => cluster.AllHosts().All(h => h.IsUp));

                ccAddress = cluster.InternalRef.GetControlConnection().Address;
                Assert.True(cluster.GetHost(ccAddress).IsUp);

                // Once all connections are created, the control connection should be usable
                WaitSimulatorConnections(testCluster, 4);
                Assert.DoesNotThrowAsync(() => cluster.InternalRef.GetControlConnection().QueryAsync("SELECT * FROM system.local"));
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

                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var hosts = cluster.AllHosts().ToArray();

                // Wait until all connections to first host are created
                await TestHelper.WaitUntilAsync(() =>
                    session.GetState().GetInFlightQueries(hosts[0]) == connectionLength).ConfigureAwait(false);

                const int overflowToNextHost = 10;
                var length = maxRequestsPerConnection * connectionLength + Environment.ProcessorCount +
                             overflowToNextHost;
                var tasks = new List<Task<RowSet>>(length);

                for (var i = 0; i < length; i++)
                {
                    tasks.Add(session.ExecuteAsync(new SimpleStatement(query)));
                }

                var results = await Task.WhenAll(tasks).ConfigureAwait(false);

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

                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var hosts = cluster.AllHosts().ToArray();

                await TestHelper.TimesLimit(() =>
                    session.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local")), 100, 16).ConfigureAwait(false);

                // Wait until all connections to all host are created
                await TestHelper.WaitUntilAsync(() =>
                {
                    var state = session.GetState();
                    return state.GetConnectedHosts().All(h => state.GetInFlightQueries(h) == connectionLength);
                }).ConfigureAwait(false);

                lbp.UseFixedOrder();

                const int busyExceptions = 10;
                var length = maxRequestsPerConnection * connectionLength * hosts.Length + Environment.ProcessorCount +
                             busyExceptions;
                var tasks = new List<Task<Exception>>(length);

                for (var i = 0; i < length; i++)
                {
                    tasks.Add(TestHelper.EatUpException(session.ExecuteAsync(new SimpleStatement(query))));
                }

                var results = await Task.WhenAll(tasks).ConfigureAwait(false);

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

        [Test]
        public async Task Should_Use_Single_Host_When_Configured_At_Statement_Level()
        {
            const string query = "SELECT * FROM system.local";
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy());

            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var firstHost = cluster.AllHosts().First();
                var lastHost = cluster.AllHosts().Last();

                // The test load-balancing policy targets always the first host
                await TestHelper.TimesLimit(async () =>
                {
                    var rs = await session.ExecuteAsync(new SimpleStatement(query)).ConfigureAwait(false);
                    Assert.AreEqual(rs.Info.QueriedHost, firstHost.Address);
                    return rs;
                }, 10, 10).ConfigureAwait(false);

                // Use a specific host
                var statement = new SimpleStatement(query).SetHost(lastHost);
                await TestHelper.TimesLimit(async () =>
                {
                    var rs = await session.ExecuteAsync(statement).ConfigureAwait(false);
                    // The queried host should be the last one
                    Assert.AreEqual(rs.Info.QueriedHost, lastHost.Address);
                    return rs;
                }, 10, 10).ConfigureAwait(false);
            }
        }

        [Test]
        public void Should_Throw_NoHostAvailableException_When_Targeting_Single_Ignored_Host()
        {
            const string query = "SELECT * FROM system.local";
            // Mark the last host as ignored
            var lbp = new TestHelper.CustomLoadBalancingPolicy(
                (cluster, ks, stmt) => cluster.AllHosts(),
                (cluster, host) => host.Equals(cluster.AllHosts().Last()) ? HostDistance.Ignored : HostDistance.Local);
            var builder = Cluster.Builder().WithLoadBalancingPolicy(lbp);

            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var lastHost = cluster.AllHosts().Last();

                // Use the last host
                var statement = new SimpleStatement(query).SetHost(lastHost);
                Parallel.For(0, 10, _ =>
                {
                    var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => session.ExecuteAsync(statement));
                    Assert.That(ex.Errors.Count, Is.EqualTo(1));
                    Assert.That(ex.Errors.First().Key, Is.EqualTo(lastHost.Address));
                });
            }
        }

        [Test]
        public async Task Should_Throw_NoHostAvailableException_When_Targeting_Single_Host_With_No_Connections()
        {
            var builder = Cluster.Builder();
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var lastHost = cluster.AllHosts().Last();

                // 1 for the control connection and 1 connection per each host 
                Assert.AreEqual(4, testCluster.GetConnectedPorts().Count);

                var simulacronNode = testCluster.GetNode(lastHost.Address);

                // Disable new connections to the first host
                await simulacronNode.DisableConnectionListener().ConfigureAwait(false);
                var connections = simulacronNode.GetConnections();

                Assert.AreEqual(1, connections.Count);
                await testCluster.DropConnection(connections[0]).ConfigureAwait(false);

                // Drop connections to the host last host
                WaitSimulatorConnections(testCluster, 3);

                Parallel.For(0, 10, _ =>
                {
                    var statement = new SimpleStatement("SELECT * FROM system.local").SetHost(lastHost)
                                                                                     .SetIdempotence(true);

                    var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => session.ExecuteAsync(statement));
                    Assert.That(ex.Errors.Count, Is.EqualTo(1));
                    Assert.That(ex.Errors.First().Key, Is.EqualTo(lastHost.Address));
                });
            }
        }

        [Test]
        public void The_Query_Plan_Should_Contain_A_Single_Host_When_Targeting_Single_Host()
        {
            var builder = Cluster.Builder();
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                const string query = "SELECT * FROM simulated_ks.table1";
                testCluster.Prime(new
                {
                    when = new { query },
                    then = new { result = "overloaded", message = "Test overloaded error" }
                });

                var session = cluster.Connect();
                var host = cluster.AllHosts().Last();

                var statement = new SimpleStatement(query).SetHost(host).SetIdempotence(true);

                // Overloaded exceptions should be retried on the next host
                // but only 1 host in the query plan is expected
                var ex = Assert.Throws<NoHostAvailableException>(() => session.Execute(statement));

                Assert.That(ex.Errors, Has.Count.EqualTo(1));
                Assert.IsInstanceOf<OverloadedException>(ex.Errors.First().Value);
                Assert.That(ex.Errors.First().Key, Is.EqualTo(host.Address));
            }
        }

        [Test]
        public async Task Should_Not_Use_The_LoadBalancingPolicy_When_Targeting_Single_Host()
        {
            var queryPlanCounter = 0;
            var lbp = new TestHelper.CustomLoadBalancingPolicy((cluster, ks, stmt) =>
            {
                Interlocked.Increment(ref queryPlanCounter);
                return cluster.AllHosts();
            });

            var builder = Cluster.Builder().WithLoadBalancingPolicy(lbp);

            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = builder.AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var host = cluster.AllHosts().Last();
                Interlocked.Exchange(ref queryPlanCounter, 0);

                await TestHelper.TimesLimit(() =>
                {
                    var statement = new SimpleStatement("SELECT * FROM system.local").SetHost(host);
                    return session.ExecuteAsync(statement);
                }, 1, 1).ConfigureAwait(false);

                Assert.Zero(Volatile.Read(ref queryPlanCounter));
            }
        }
    }
}