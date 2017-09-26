using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using SCluster = Cassandra.IntegrationTests.TestClusterManagement.Simulacron.Cluster;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class PoolShortTests : TestGlobals
    {
        private static SimulacronManager _simulacronManager;
        [TearDown]
        public void OnTearDown()
        {
            TestClusterManager.TryRemove();
            if (_simulacronManager != null)
            {
                _simulacronManager.Stop();
            }
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
            //start scassandra
            _simulacronManager = SimulacronManager.Instance;
            _simulacronManager.Start();
            var sCluster = SCluster.Create("1", TestClusterManager.CassandraVersionText, "poolShort", false, 1);
            var contactPoint = sCluster.InitialContactPoint;

            const int connectionLength = 4;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint.Item1)
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
                    sCluster.DropConnection(ports.Last().Item1, ports.Last().Item2).Wait();
                    // Host pool could have between pool.OpenConnections - i and pool.OpenConnections - i - 1
                    TestHelper.WaitUntil(() => pool.OpenConnections >= 4 - index - 1 && pool.OpenConnections <= 4 - index);
                    Assert.LessOrEqual(pool.OpenConnections, 4 - index);
                    Assert.GreaterOrEqual(pool.OpenConnections, 4 - index - 1);
                    Assert.IsTrue(h1.IsUp);
                }
                WaitSimulatorConnections(sCluster, 2);
                ports = sCluster.GetConnectedPorts();
                Assert.AreEqual(2, ports.Count);
                sCluster.DropConnection(ports.Last().Item1, ports.Last().Item2).Wait();
                sCluster.DropConnection(ports.First().Item1, ports.First().Item2).Wait();
                TestHelper.WaitUntil(() => pool.OpenConnections == 0 && !h1.IsUp);
                Assert.IsFalse(h1.IsUp);
            }
        }

        /// <summary>
        /// Waits for the simulator to have the expected number of connections
        /// </summary>
        private static void WaitSimulatorConnections(SCluster sCluster, int expected)
        {
            TestHelper.WaitUntil(() => sCluster.GetConnectedPorts().Count == expected);
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
    }
}