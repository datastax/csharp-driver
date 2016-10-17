using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class ReconnectionTests : TestGlobals
    {
        /// Tests that reconnection attempts are made multiple times in the background
        ///
        /// Reconnection_Attempted_Multiple_Times tests that the driver automatically reschedules host reconnections using 
        /// timers in the background multiple times. It first creates a Cassandra cluster with a single node, and verifies
        /// that the driver considers it as up. It then stops the single node and verifies that the driver considers the node
        /// as down, by both executing a query and retrieving a NoHostAvailableException, and checking the host status manually.
        /// It then waits 15 seconds before verifying once more that the host is seen as down by the driver. Finally it restarts
        /// the single node, waits 5 seconds for the timer-based reconnection policy to kick in, and verifies that the driver sees 
        /// the host as back up.
        ///
        /// @since 2.7.0
        /// @jira_ticket CSHARP-280
        /// @expected_result The host should be attempted to be reconnected multiple times in the background
        ///
        /// @test_assumptions
        ///    - A Cassandra cluster with a single node
        /// @test_category connection:reconnection
        [Test]
        public void Reconnection_Attempted_Multiple_Times()
        {
            var testCluster = TestClusterManager.CreateNew();

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithPoolingOptions(
                                            new PoolingOptions()
                                                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                                .SetHeartBeatInterval(0))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(2000))
                                        .Build())
            {
                var session = (Session)cluster.Connect();
                TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 10);
                //Another session to have multiple pools
                var dummySession = cluster.Connect();
                TestHelper.Invoke(() => dummySession.Execute("SELECT * FROM system.local"), 10);
                var host = cluster.AllHosts().First();
                var upCounter = 0;
                var downCounter = 0;
                cluster.Metadata.HostsEvent += (sender, e) =>
                {
                    if (e.What == HostsEventArgs.Kind.Up)
                    {
                        Interlocked.Increment(ref upCounter);
                        return;
                    }
                    Interlocked.Increment(ref downCounter);
                };
                Assert.True(host.IsUp);
                Trace.TraceInformation("Stopping node");
                testCluster.Stop(1);
                // Make sure the node is considered down
                Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT * FROM system.local"));
                Assert.False(host.IsUp);
                Assert.AreEqual(1, Volatile.Read(ref downCounter));
                Assert.AreEqual(0, Volatile.Read(ref upCounter));
                Trace.TraceInformation("Waiting for 15 seconds");
                Thread.Sleep(15000);
                Assert.False(host.IsUp);
                Trace.TraceInformation("Restarting node");
                testCluster.Start(1);
                Trace.TraceInformation("Waiting for 5 seconds");
                Thread.Sleep(5000);
                Assert.True(host.IsUp);
                Assert.AreEqual(1, Volatile.Read(ref downCounter));
                Assert.AreEqual(1, Volatile.Read(ref upCounter));
            }
        }

        /// Tests that reconnection attempts are made multiple times in the background
        ///
        /// Reconnection_Attempted_Multiple_Times_On_Multiple_Nodes tests that the driver automatically reschedules host reconnections using 
        /// timers in the background multiple times. It first creates a Cassandra cluster with 2 nodes, and verifies
        /// that the driver considers it as up. It then stops the two nodes and verifies that the driver considers the node
        /// as down.
        /// It then waits a few seconds before restarting the single node, waits 5 seconds for the timer-based reconnection policy 
        /// to kick in, and verifies that the driver sees the hosts as back up.
        /// The test run is repeated multiple times.
        ///
        /// @since 3.0.0
        /// @jira_ticket CSHARP-386
        /// @expected_result The hosts should be attempted to be reconnected multiple times in the background
        ///
        /// @test_category connection:reconnection
        [Test, Repeat(3)]
        public void Reconnection_Attempted_Multiple_Times_On_Multiple_Nodes()
        {
            var testCluster = TestClusterManager.CreateNew(2);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithPoolingOptions(
                                            new PoolingOptions()
                                                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                                .SetHeartBeatInterval(0))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                                        .Build())
            {
                var session = cluster.Connect();
                TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 10);
                //Another session to have multiple pools
                var dummySession = cluster.Connect();
                TestHelper.Invoke(() => dummySession.Execute("SELECT * FROM system.local"), 10);
                var host1 = cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 1);
                var host2 = cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 2);
                var upCounter = new ConcurrentDictionary<byte, int>();
                var downCounter = new ConcurrentDictionary<byte, int>();
                cluster.Metadata.HostsEvent += (sender, e) =>
                {
                    if (e.What == HostsEventArgs.Kind.Up)
                    {
                        upCounter.AddOrUpdate(TestHelper.GetLastAddressByte(e.Address), 1, (k, v) => ++v);
                        return;
                    }
                    downCounter.AddOrUpdate(TestHelper.GetLastAddressByte(e.Address), 1, (k, v) => ++v);
                };
                Assert.True(host1.IsUp);
                Trace.TraceInformation("Stopping node #1");
                testCluster.Stop(1);
                // Make sure the node is considered down
                Assert.DoesNotThrow(() => TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 6));
                Thread.Sleep(1000);
                Assert.False(host1.IsUp);
                Assert.AreEqual(1, downCounter.GetOrAdd(1, 0));
                Assert.AreEqual(0, upCounter.GetOrAdd(1, 0));
                Trace.TraceInformation("Stopping node #2");
                testCluster.Stop(2);
                Assert.Throws<NoHostAvailableException>(() => TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 6));
                Thread.Sleep(1000);
                Assert.False(host2.IsUp);
                Assert.AreEqual(1, downCounter.GetOrAdd(1, 0));
                Assert.AreEqual(0, upCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, downCounter.GetOrAdd(2, 0));
                Assert.AreEqual(0, upCounter.GetOrAdd(2, 0));
                Trace.TraceInformation("Waiting for few seconds");
                Thread.Sleep(8000);
                Assert.False(host1.IsUp);
                Assert.False(host2.IsUp);
                Trace.TraceInformation("Restarting node #1");
                testCluster.Start(1);
                Trace.TraceInformation("Restarting node #2");
                testCluster.Start(2);
                Trace.TraceInformation("Waiting for few more seconds");
                Thread.Sleep(6000);
                Assert.True(host1.IsUp);
                Assert.True(host2.IsUp);
                Assert.AreEqual(1, downCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, upCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, downCounter.GetOrAdd(2, 0));
                Assert.AreEqual(1, upCounter.GetOrAdd(2, 0));
            }
        }

        [Test]
        public void Executions_After_Reconnection_Resizes_Pool()
        {
            var testCluster = TestClusterManager.CreateNew(2);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithPoolingOptions(
                                            new PoolingOptions()
                                                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                                .SetMaxConnectionsPerHost(HostDistance.Local, 2)
                                                .SetHeartBeatInterval(0))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                                        .Build())
            {
                var session1 = (Session)cluster.Connect();
                var session2 = (Session)cluster.Connect();
                TestHelper.Invoke(() => session1.Execute("SELECT * FROM system.local"), 10);
                TestHelper.Invoke(() => session2.Execute("SELECT * FROM system.local"), 10);
                var host1 = cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 1);
                var host2 = cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 2);
                var upCounter = new ConcurrentDictionary<byte, int>();
                var downCounter = new ConcurrentDictionary<byte, int>();
                cluster.Metadata.HostsEvent += (sender, e) =>
                {
                    if (e.What == HostsEventArgs.Kind.Up)
                    {
                        upCounter.AddOrUpdate(TestHelper.GetLastAddressByte(e.Address), 1, (k, v) => ++v);
                        return;
                    }
                    downCounter.AddOrUpdate(TestHelper.GetLastAddressByte(e.Address), 1, (k, v) => ++v);
                };
                Assert.True(host1.IsUp);
                Assert.True(host2.IsUp);
                Trace.TraceInformation("Stopping node #1");
                testCluster.Stop(1);
                Trace.TraceInformation("Stopping node #2");
                testCluster.Stop(2);
                //Force to be considered down
                Assert.Throws<NoHostAvailableException>(() => session1.Execute("SELECT * FROM system.local"));
                Assert.AreEqual(1, downCounter.GetOrAdd(1, 0));
                Assert.AreEqual(0, upCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, downCounter.GetOrAdd(2, 0));
                Assert.AreEqual(0, upCounter.GetOrAdd(2, 0));
                Trace.TraceInformation("Restarting node #1");
                testCluster.Start(1);
                Trace.TraceInformation("Restarting node #2");
                testCluster.Start(2);
                Trace.TraceInformation("Waiting for few more seconds");
                Thread.Sleep(6000);
                Assert.True(host1.IsUp);
                Assert.True(host2.IsUp);
                Assert.AreEqual(1, downCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, upCounter.GetOrAdd(1, 0));
                Assert.AreEqual(1, downCounter.GetOrAdd(2, 0));
                Assert.AreEqual(1, upCounter.GetOrAdd(2, 0));
                TestHelper.Invoke(() => session1.Execute("SELECT * FROM system.local"), 10);
                TestHelper.Invoke(() => session2.Execute("SELECT * FROM system.local"), 10);
                Trace.TraceInformation("Waiting for few more seconds");
                Thread.Sleep(6000);
                var pool1 = session1.GetOrCreateConnectionPool(host1, HostDistance.Local);
                Assert.AreEqual(2, pool1.OpenConnections);
                var pool2 = session1.GetOrCreateConnectionPool(host2, HostDistance.Local);
                Assert.AreEqual(2, pool2.OpenConnections);
            }
        }
    }
}
