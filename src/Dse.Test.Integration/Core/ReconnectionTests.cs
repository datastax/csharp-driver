//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Dse.SessionManagement;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using Dse.Test.Unit;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [Category("short")]
    public class ReconnectionTests : TestGlobals
    {
        private SimulacronCluster _testCluster;
        private Lazy<ITestCluster> _realCluster = new Lazy<ITestCluster>(() => TestClusterManagement.TestClusterManager.CreateNew(2));

        [TearDown]
        public void TearDown()
        {
            _testCluster?.Dispose();
            _testCluster = null;

            if (_realCluster.IsValueCreated)
            {
                TestClusterManager.TryRemove();
                _realCluster = new Lazy<ITestCluster>(
                    () => TestClusterManagement.TestClusterManager.CreateNew(2, new TestClusterOptions { UseVNodes = true}));
            }
        }

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
            _testCluster = SimulacronCluster.CreateNew(1);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
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
                var node = _testCluster.GetNodes().First(); 
                node.Stop().GetAwaiter().GetResult();
                // Make sure the node is considered down
                TestHelper.RetryAssert(() =>
                {
                    Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT * FROM system.local"));
                    Assert.False(host.IsUp);
                    Assert.AreEqual(1, Volatile.Read(ref downCounter));
                    Assert.AreEqual(0, Volatile.Read(ref upCounter));
                }, 100, 200);
                Trace.TraceInformation("Restarting node");
                node.Start().GetAwaiter().GetResult();
                Trace.TraceInformation("Waiting up to 20s");
                TestHelper.RetryAssert(() =>
                {
                    Assert.True(host.IsUp);
                    Assert.AreEqual(1, Volatile.Read(ref downCounter));
                    Assert.AreEqual(1, Volatile.Read(ref upCounter));
                }, 100, 200);
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
        [Test]
        [Repeat(3)]
        public void Reconnection_Attempted_Multiple_Times_On_Multiple_Nodes()
        {
            _testCluster = SimulacronCluster.CreateNew(2);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
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
                var hosts = cluster.AllHosts().ToList();
                var host1 = hosts[0];
                var host2 = hosts[1];
                var upCounter = new ConcurrentDictionary<string, int>();
                var downCounter = new ConcurrentDictionary<string, int>();
                cluster.Metadata.HostsEvent += (sender, e) =>
                {
                    if (e.What == HostsEventArgs.Kind.Up)
                    {
                        upCounter.AddOrUpdate(e.Address.ToString(), 1, (k, v) => ++v);
                        return;
                    }
                    downCounter.AddOrUpdate(e.Address.ToString(), 1, (k, v) => ++v);
                };
                Assert.True(host1.IsUp);
                Trace.TraceInformation("Stopping node #1");
                var nodes = _testCluster.GetNodes().ToArray();
                nodes[0].Stop().GetAwaiter().GetResult();
                // Make sure the node is considered down
                TestHelper.RetryAssert(() =>
                {
                    Assert.DoesNotThrow(() => TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 6));
                    Assert.False(host1.IsUp);
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(0, upCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                }, 100, 100);
                Trace.TraceInformation("Stopping node #2");
                nodes[1].Stop().GetAwaiter().GetResult();
                
                TestHelper.RetryAssert(() =>
                {
                    Assert.Throws<NoHostAvailableException>(() => TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 6));
                    Assert.False(host2.IsUp);
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(0, upCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                    Assert.AreEqual(0, upCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                    Assert.False(host1.IsUp);
                    Assert.False(host2.IsUp);
                }, 100, 100);

                Trace.TraceInformation("Restarting node #1");
                nodes[0].Start().GetAwaiter().GetResult();
                Trace.TraceInformation("Restarting node #2");
                nodes[1].Start().GetAwaiter().GetResult();
                Trace.TraceInformation("Waiting for few more seconds");
                TestHelper.RetryAssert(() =>
                {
                    Assert.True(host1.IsUp && host2.IsUp);
                    Assert.True(host1.IsUp, "Host 1 should be UP after restarting");
                    Assert.True(host2.IsUp, "Host 2 should be UP after restarting");
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, upCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                    Assert.AreEqual(1, upCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                }, 100, 200);
            }
        }

        [Test]
        public void Executions_After_Reconnection_Resizes_Pool()
        {
            _testCluster = SimulacronCluster.CreateNew(2);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
                                        .WithPoolingOptions(
                                            new PoolingOptions()
                                                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                                .SetMaxConnectionsPerHost(HostDistance.Local, 2)
                                                .SetHeartBeatInterval(0))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                                        .Build())
            {
                var session1 = (IInternalSession)cluster.Connect();
                var session2 = (IInternalSession)cluster.Connect();
                TestHelper.Invoke(() => session1.Execute("SELECT * FROM system.local"), 10);
                TestHelper.Invoke(() => session2.Execute("SELECT * FROM system.local"), 10);
                var hosts = cluster.AllHosts().ToList();
                var host1 = hosts[0];
                var host2 = hosts[1];
                var upCounter = new ConcurrentDictionary<string, int>();
                var downCounter = new ConcurrentDictionary<string, int>();
                cluster.Metadata.HostsEvent += (sender, e) =>
                {
                    if (e.What == HostsEventArgs.Kind.Up)
                    {
                        upCounter.AddOrUpdate(e.Address.ToString(), 1, (k, v) => ++v);
                        return;
                    }
                    downCounter.AddOrUpdate(e.Address.ToString(), 1, (k, v) => ++v);
                };
                var nodes = _testCluster.GetNodes().ToList();
                Assert.True(host1.IsUp);
                Assert.True(host2.IsUp);
                Trace.TraceInformation("Stopping node #1");
                nodes[0].Stop().GetAwaiter().GetResult();
                Trace.TraceInformation("Stopping node #2");
                nodes[1].Stop().GetAwaiter().GetResult();
                //Force to be considered down
                TestHelper.RetryAssert(() =>
                {
                    Assert.Throws<NoHostAvailableException>(() => session1.Execute("SELECT * FROM system.local"));
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(0, upCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                    Assert.AreEqual(0, upCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                }, 100, 200);
                Trace.TraceInformation("Restarting node #1");
                nodes[0].Start().GetAwaiter().GetResult();
                Trace.TraceInformation("Restarting node #2");
                nodes[1].Start().GetAwaiter().GetResult();
                Trace.TraceInformation("Waiting for few more seconds");
                TestHelper.RetryAssert(() =>
                {
                    Assert.True(host1.IsUp);
                    Assert.True(host2.IsUp);
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, upCounter.GetOrAdd(nodes[0].ContactPoint, 0));
                    Assert.AreEqual(1, downCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                    Assert.AreEqual(1, upCounter.GetOrAdd(nodes[1].ContactPoint, 0));
                }, 100, 200);
                TestHelper.Invoke(() => session1.Execute("SELECT * FROM system.local"), 10);
                TestHelper.Invoke(() => session2.Execute("SELECT * FROM system.local"), 10);
                var pool1 = session1.GetOrCreateConnectionPool(host1, HostDistance.Local);
                Assert.AreEqual(2, pool1.OpenConnections);
                var pool2 = session1.GetOrCreateConnectionPool(host2, HostDistance.Local);
                Assert.AreEqual(2, pool2.OpenConnections);
            }
        }

        [Category("realcluster")]
        [Test]
        public void Should_UseNewHostInQueryPlans_When_HostIsDecommissionedAndJoinsAgain()
        {
            var testCluster = _realCluster.Value;
            using (var cluster = 
                 Cluster.Builder()
                        .AddContactPoint(testCluster.InitialContactPoint)
                        .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                        .WithPoolingOptions(
                            new PoolingOptions()
                                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                .SetMaxConnectionsPerHost(HostDistance.Local, 2))
                        .Build())
            {
                var session = (IInternalSession)cluster.Connect();
                session.CreateKeyspaceIfNotExists("testks");
                session.ChangeKeyspace("testks");
                session.Execute("CREATE TABLE test_table (id text, PRIMARY KEY (id))");

                // Assert that there are 2 pools, one for each host
                var hosts = session.Cluster.AllHosts().ToList();
                var pool1 = session.GetExistingPool(hosts[0].Address);
                Assert.AreEqual(2, pool1.OpenConnections);
                var pool2 = session.GetExistingPool(hosts[1].Address);
                Assert.AreEqual(2, pool2.OpenConnections);

                // Assert that both hosts are used in queries
                var set = new HashSet<IPEndPoint>();
                foreach (var i in Enumerable.Range(1, 100))
                {
                    var rs = session.Execute($"INSERT INTO test_table(id) VALUES ('{i}')");
                    set.Add(rs.Info.QueriedHost);
                }
                Assert.AreEqual(2, set.Count);
                
                // Decommission node
                if (TestClusterManager.DseVersion.Major < 5 ||
                    (TestClusterManager.DseVersion.Major == 5 && TestClusterManager.DseVersion.Minor < 1))
                {
                    testCluster.DecommissionNode(1);
                }
                else
                {
                    testCluster.DecommissionNodeForcefully(1);
                }
                testCluster.Stop(1);
                
                // Assert that only one host is used in queries
                set.Clear();
                foreach (var i in Enumerable.Range(1, 100))
                {
                    var rs = session.Execute($"INSERT INTO test_table(id) VALUES ('{i}')");
                    set.Add(rs.Info.QueriedHost);
                }
                Assert.AreEqual(1, set.Count);

                var removedHost = hosts.Single(h => !h.Address.Equals(set.First()));

                // Bring back the decommissioned node
                testCluster.Start(1, "--jvm_arg=\"-Dcassandra.override_decommission=true\"");
                
                // Assert that there are 2 hosts
                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(2, cluster.AllHosts().Count);
                }, 1000, 180);
                
                // Assert that queries use both hosts again
                set.Clear();
                var idx = 1;
                TestHelper.RetryAssert(() =>
                {
                    var rs = session.Execute($"INSERT INTO test_table(id) VALUES ('{idx++}')");
                    set.Add(rs.Info.QueriedHost);
                    Assert.AreEqual(2, set.Count);
                }, 10, 3000);
                
                pool2 = session.GetExistingPool(removedHost.Address);
                Assert.IsNotNull(pool2);
                Assert.AreEqual(2, pool2.OpenConnections);
            }
        }

        [Category("realcluster")]
        [Test]
        public void Should_UpdateHosts_When_HostIpChanges()
        {
            var oldIp = $"{_realCluster.Value.ClusterIpPrefix}3";
            var newIp = $"{_realCluster.Value.ClusterIpPrefix}4";
            var oldEndPoint = new IPEndPoint(IPAddress.Parse(oldIp), ProtocolOptions.DefaultPort);
            var newEndPoint = new IPEndPoint(IPAddress.Parse(newIp), ProtocolOptions.DefaultPort);
            var addresses = new[]
            {
                $"{_realCluster.Value.ClusterIpPrefix}1",
                $"{_realCluster.Value.ClusterIpPrefix}2",
                oldIp
            };
            
            var newAddresses = new[]
            {
                $"{_realCluster.Value.ClusterIpPrefix}1",
                $"{_realCluster.Value.ClusterIpPrefix}2",
                newIp
            };

            _realCluster.Value.BootstrapNode(3);

            using (var cluster = Cluster.Builder().AddContactPoint(_realCluster.Value.InitialContactPoint).Build())
            {
                var session = (IInternalSession)cluster.Connect();
                session.CreateKeyspaceIfNotExists("ks1");
                var hosts = session.Cluster.AllHosts().OrderBy(h => h.Address.ToString()).ToArray();
                Assert.AreEqual(3, hosts.Length);
                Assert.AreEqual(3, session.GetPools().Count());
                Assert.IsNotNull(session.GetExistingPool(oldEndPoint));
                Assert.IsTrue(hosts.Select(h => h.Address.Address.ToString()).SequenceEqual(addresses));
                var tokenMap = session.Cluster.Metadata.TokenToReplicasMap;
                var oldHostTokens = tokenMap.GetByKeyspace("ks1")
                                     .Where(kvp =>
                                         kvp.Value.First().Address.Address.ToString().Equals(oldIp));

                _realCluster.Value.Stop(3);
                _realCluster.Value.UpdateConfig(
                    3, 
                    $"listen_address: {_realCluster.Value.ClusterIpPrefix}4", 
                    $"rpc_address: {_realCluster.Value.ClusterIpPrefix}4");

                _realCluster.Value.Start(3, "--skip-wait-other-notice", "127.0.0.4");

                TestHelper.RetryAssert(
                    () =>
                    {
                        Assert.AreEqual(1,
                            session.Cluster.AllHosts()
                                   .Count(h => h.Address.Address.ToString().Equals($"{_realCluster.Value.ClusterIpPrefix}4")),
                            string.Join(";", session.Cluster.AllHosts().Select(h => h.Address.Address.ToString())));
                        Assert.AreNotSame(tokenMap, session.Cluster.Metadata.TokenToReplicasMap);
                    },
                    100,
                    100);
                
                var newTokenMap = session.Cluster.Metadata.TokenToReplicasMap;
                var newHostTokens = newTokenMap.GetByKeyspace("ks1")
                                            .Where(kvp =>
                                                kvp.Value.First().Address.Address.ToString().Equals(newIp)).ToList();

                hosts = session.Cluster.AllHosts().ToArray();
                Assert.AreEqual(3, hosts.Length);
                Assert.IsTrue(hosts.Select(h => h.Address.Address.ToString()).SequenceEqual(newAddresses));
                Assert.IsTrue(newHostTokens.Select(h => h.Key).SequenceEqual(oldHostTokens.Select(h => h.Key)));

                // force session to open connection to the new node
                foreach (var i in Enumerable.Range(0, 5))
                {
                    session.Execute("SELECT * FROM system.local");
                }

                Assert.AreEqual(3, session.GetPools().Count());
                Assert.IsNull(session.GetExistingPool(oldEndPoint));
                Assert.IsNotNull(session.GetExistingPool(newEndPoint));
            }
        }
    }
}
