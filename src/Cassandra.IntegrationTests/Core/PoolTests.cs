//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class PoolTests : TestGlobals
    {
        protected TraceLevel OriginalTraceLevel;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            OriginalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Diagnostics.CassandraTraceSwitch.Level = OriginalTraceLevel;
        }

        [Test]
        public void ReconnectionRecyclesPool()
        {
            var policy = new ConstantReconnectionPolicy(5000);

            var nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            nonShareableTestCluster.Builder = new Builder().WithReconnectionPolicy(policy);
            nonShareableTestCluster.InitClient(); // this will replace the existing session using the newly assigned Builder instance
            var session = (Session)nonShareableTestCluster.Session;

            var hosts = new List<IPEndPoint>();
            for (var i = 0; i < 50; i++)
            {
                var rs = session.Execute("SELECT * FROM system.local");
                if (i == 20)
                {
                    nonShareableTestCluster.StopForce(2);
                }
                else if (i == 30)
                {
                    nonShareableTestCluster.Start(2);
                    Thread.Sleep(5000);
                }
                hosts.Add(rs.Info.QueriedHost);
            }

            var pool = session.GetOrCreateConnectionPool(TestHelper.CreateHost(nonShareableTestCluster.InitialContactPoint), HostDistance.Local);
            var connections = pool.OpenConnections.ToArray();
            var expectedCoreConnections = nonShareableTestCluster.Cluster.Configuration
                .GetPoolingOptions(connections.First().ProtocolVersion)
                .GetCoreConnectionsPerHost(HostDistance.Local);
            Assert.AreEqual(expectedCoreConnections, connections.Length);
            Assert.True(connections.All(c => !c.IsClosed));
        }

        /// <summary>
        /// Executes statements in parallel while killing nodes, validates that there are no issues failing over to remaining, available nodes
        /// </summary>
        [Test]
        public void FailoverTest()
        {
            var parallelOptions = new ParallelOptions();
            parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
            parallelOptions.MaxDegreeOfParallelism = 100;

            var policy = new ConstantReconnectionPolicy(int.MaxValue);
            var nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(4, 1, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(nonShareableTestCluster.InitialContactPoint).WithReconnectionPolicy(policy).Build())
            {
                var session = cluster.Connect();
                // Check query to host distribution before killing nodes
                var queriedHosts = new List<string>();
                DateTime futureDateTime = DateTime.Now.AddSeconds(120);
                while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
                {
                    var rs = session.Execute("SELECT * FROM system.local");
                    queriedHosts.Add(rs.Info.QueriedHost.ToString());
                    Thread.Sleep(50);
                }
                Assert.AreEqual(4, (from singleHost in queriedHosts select singleHost).Distinct().Count(), "All hosts should have been queried!");

                // Create List of actions
                Action selectAction = () =>
                {
                    var rs = session.Execute("SELECT * FROM system.local");
                    Assert.Greater(rs.Count(), 0);
                };
                var actions = new List<Action>();
                for (var i = 0; i < 100; i++)
                {
                    actions.Add(selectAction);
                }

                //kill some nodes.
                actions.Insert(20, () => nonShareableTestCluster.StopForce(1));
                actions.Insert(20, () => nonShareableTestCluster.StopForce(2));
                actions.Insert(80, () => nonShareableTestCluster.StopForce(3));

                //Execute in parallel more than 100 actions
                Parallel.Invoke(parallelOptions, actions.ToArray());

                // Wait for the nodes to be killed
                TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "1", nonShareableTestCluster.Cluster, 20);
                TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "2", nonShareableTestCluster.Cluster, 20);
                TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "3", nonShareableTestCluster.Cluster, 20);

                // Execute some more SELECTs
                for (var i = 0; i < 250; i++)
                {
                    var rowSet2 = session.Execute("SELECT * FROM system.local");
                    Assert.Greater(rowSet2.Count(), 0);
                    StringAssert.StartsWith(nonShareableTestCluster.ClusterIpPrefix + "4", rowSet2.Info.QueriedHost.ToString());
                }
            }
        }

        /// <summary>
        /// Executes statements in parallel while killing nodes, validates that there are no issues failing over to remaining, available nodes
        /// </summary>
        [Test]
        public void FailoverThenReconnect()
        {
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(),
                MaxDegreeOfParallelism = 100
            };

            var policy = new ConstantReconnectionPolicy(500);
            var nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(4, 1, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(nonShareableTestCluster.InitialContactPoint).WithReconnectionPolicy(policy).Build())
            {
                var session = cluster.Connect();
                // Check query to host distribution before killing nodes
                var queriedHosts = new List<string>();
                DateTime futureDateTime = DateTime.Now.AddSeconds(120);
                while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
                {
                    var rs = session.Execute("SELECT * FROM system.local");
                    queriedHosts.Add(rs.Info.QueriedHost.ToString());
                    Thread.Sleep(50);
                }
                Assert.AreEqual(4, (from singleHost in queriedHosts select singleHost).Distinct().Count(), "All hosts should have been queried!");

                // Create list of actions
                Action selectAction = () =>
                {
                    var rs = session.Execute("SELECT * FROM system.local");
                    Assert.Greater(rs.Count(), 0);
                };
                var actions = new List<Action>();
                for (var i = 0; i < 100; i++)
                {
                    actions.Add(selectAction);
                    //Check that the control connection is using first host
                    StringAssert.StartsWith(nonShareableTestCluster.ClusterIpPrefix + "1", nonShareableTestCluster.Cluster.Metadata.ControlConnection.Address.ToString());

                    //Kill some nodes
                    //Including the one used by the control connection
                    actions.Insert(20, () => nonShareableTestCluster.Stop(1));
                    actions.Insert(20, () => nonShareableTestCluster.Stop(2));
                    actions.Insert(80, () => nonShareableTestCluster.Stop(3));

                    //Execute in parallel more than 100 actions
                    Parallel.Invoke(parallelOptions, actions.ToArray());

                    //Wait for the nodes to be killed
                    TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "1", nonShareableTestCluster.Cluster, 20);
                    TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "2", nonShareableTestCluster.Cluster, 20);
                    TestUtils.WaitForDown(nonShareableTestCluster.ClusterIpPrefix + "3", nonShareableTestCluster.Cluster, 20);

                    actions = new List<Action>();
                    for (var j = 0; j < 100; j++)
                    {
                        actions.Add(selectAction);
                    }

                    //Check that the control connection is using first host
                    //bring back some nodes
                    actions.Insert(3, () => nonShareableTestCluster.Start(3));
                    actions.Insert(50, () => nonShareableTestCluster.Start(2));
                    actions.Insert(50, () => nonShareableTestCluster.Start(1));

                    //Execute in parallel more than 100 actions
                    Trace.TraceInformation("Start invoking with restart nodes");
                    Parallel.Invoke(parallelOptions, actions.ToArray());

                    //Wait for the nodes to be restarted
                    TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
                    TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);
                    TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "3", DefaultCassandraPort, 30);

                    queriedHosts.Clear();
                    // keep querying hosts until they are all queried, or time runs out
                    futureDateTime = DateTime.Now.AddSeconds(120);
                    while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
                    {
                        var rs = session.Execute("SELECT * FROM system.local");
                        queriedHosts.Add(rs.Info.QueriedHost.ToString());
                        Thread.Sleep(50);
                    }
                    //Check that one of the restarted nodes were queried
                    Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, queriedHosts);
                    Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, queriedHosts);
                    Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, queriedHosts);
                    Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "4:" + DefaultCassandraPort, queriedHosts);
                    //Check that the control connection is still using last host
                    StringAssert.StartsWith(nonShareableTestCluster.ClusterIpPrefix + "4", nonShareableTestCluster.Cluster.Metadata.ControlConnection.Address.ToString());
                }
            }
        }

        /// <summary>
        /// Validates that the client adds the newly bootstrapped node and eventually queries from it
        /// </summary>
        [Test]
        public void BootstrappedNode()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                testCluster.BootstrapNode(2);
                var queried = false;
                for (var i = 0; i < 10; i++)
                {
                    var rs = session.Execute("SELECT key FROM system.local");
                    if (rs.Info.QueriedHost.Address.ToString() == testCluster.ClusterIpPrefix + 2)
                    {
                        queried = true;
                        break;
                    }
                    Thread.Sleep(1000);
                }
                Assert.True(queried, "Newly bootstrapped node should be queried");
            }
        }

        [Test]
        public void InitialKeyspaceRaceTest()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);
            using (var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                //using a keyspace
                .WithDefaultKeyspace("system")
                //lots of connections per host
                .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 30))
                .Build())
            {
                var session = cluster.Connect();
                //Try to be force a race condition
                TestHelper.ParallelInvoke(() =>
                {
                    var t = session.ExecuteAsync(new SimpleStatement("SELECT * FROM local"));
                    t.Wait();
                }, 1000);
                var actions = new Task[1000];
                for (var i = 0; i < actions.Length; i++)
                {
                    actions[i] = session.ExecuteAsync(new SimpleStatement("SELECT * FROM local"));
                }
                // ReSharper disable once CoVariantArrayConversion
                Task.WaitAll(actions);
            }
        }

        [Test]
        public void ConnectWithWrongKeyspaceNameTest()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);

            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                //using a keyspace that does not exists
                .WithDefaultKeyspace("DOES_NOT_EXISTS_" + Randomm.RandomAlphaNum(12))
                .Build();

            Assert.Throws<InvalidQueryException>(() => cluster.Connect());
            Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES"));
        }

        [Test]
        public void Connect_With_Ssl_Test()
        {
            //use ssl
            var testCluster = TestClusterManager.GetTestCluster(1, 0, false, 1, true, false, 0, null, true);

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
        public void ConnectShouldResolveNames()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);

            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .Build();

            testCluster.Cluster.Connect("system");
            StringAssert.StartsWith(testCluster.InitialContactPoint, cluster.AllHosts().First().Address.ToString());
        }

        [Test]
        public void HeartbeatShouldDetectNodeDown()
        {
            //Execute a couple of time
            //Kill connections the node silently
            //Do nothing for a while
            //Check if the node is considered as down
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);

            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithPoolingOptions(
                                     new PoolingOptions()
                                         .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                         .SetHeartBeatInterval(500))
                                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(Int32.MaxValue))
                                 .Build();
            var session = (Session) cluster.Connect();
            for (var i = 0; i < 6; i++)
            {
                session.Execute("SELECT * FROM system.local");
            }
            var host = cluster.AllHosts().First();
            var pool = session.GetOrCreateConnectionPool(host, HostDistance.Local);
            Trace.TraceInformation("Killing connections");
            foreach (var c in pool.OpenConnections)
            {
                c.Kill();
            }
            Trace.TraceInformation("Waiting");
            for (var i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
            }
            Assert.False(cluster.AllHosts().ToList()[0].IsUp);
        }

        /// <summary>
        /// Tests that if no host is available at Cluster.Init(), it will initialize next time it is invoked
        /// </summary>
        [Test]
        public void ClusterInitializationRecoversFromNoHostAvailable()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(1);
            nonShareableTestCluster.StopForce(1);
            var cluster = Cluster.Builder()
                .AddContactPoint(nonShareableTestCluster.InitialContactPoint)
                .Build();
            //initially it will throw as there is no node reachable
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect());

            // wait for the node to be up
            nonShareableTestCluster.Start(1);
            DateTime timeInTheFuture = DateTime.Now.AddSeconds(60);
            bool clusterIsUp = false;
            while (!clusterIsUp && DateTime.Now < timeInTheFuture)
            {
                try
                {
                    cluster.Connect();
                    clusterIsUp = true;
                }
                catch (NoHostAvailableException) { }
            }

            //Now the node is ready to accept connections
            var session = cluster.Connect("system");
            TestHelper.ParallelInvoke(() => session.Execute("SELECT * from local"), 20);
        }

        /// <summary>
        /// Tests that the reconnection attempt (on a dead node) is attempted only once per try (when allowed by the reconnection policy).
        /// </summary>
        [Test]
        [Timeout(120000)]
        public void ReconnectionAttemptedOnlyOnce()
        {
            const int reconnectionDelay = 5000;
            const int waitTime = reconnectionDelay * 3 + 4000;
            var nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2, DefaultMaxClusterCreateRetries, true, false);
            var cluster = Cluster.Builder()
                .AddContactPoint(nonShareableTestCluster.InitialContactPoint)
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelay))
                .Build();
            var connectionAttempts = 0;
            cluster.Metadata.Hosts.Down += (h, s) =>
            {
                //Every time there is a connection attempt, it is marked as down
                connectionAttempts++;
                Trace.TraceInformation("--Considered as down at " + DateTime.Now.ToString("hh:mm:ss.fff"));
            };
            nonShareableTestCluster.Stop(2);
            var session = cluster.Connect();
            TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 10);
            Assert.AreEqual(1, connectionAttempts);
            var watch = new Stopwatch();
            watch.Start();
            Action action = () =>
            {
                if (watch.ElapsedMilliseconds < waitTime)
                {
                    session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local"));
                }
            };
            var waitHandle = new AutoResetEvent(false);
            var t = new Timer(s =>
            {
                waitHandle.Set();
            }, null, waitTime, Timeout.Infinite);
            var parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = 50 };
            while (watch.ElapsedMilliseconds < waitTime)
            {
                Trace.TraceInformation("Executing multiple times");
                Parallel.Invoke(parallelOptions, Enumerable.Repeat(action, 20).ToArray());
                Thread.Sleep(500);
            }
            Assert.True(waitHandle.WaitOne(waitTime), "Wait time passed but it was not signaled");
            t.Dispose();
            Assert.AreEqual(4, connectionAttempts);
        }

        /// Tests that reconnection attempts are made automatically in the background
        ///
        /// Reconnection_Attempts_Are_Made_In_The_Background tests that the driver automatically reschedules host
        /// reconnections using timers in the background. It first creates a Cassandra cluster with 2 nodes, and verifies
        /// that the control connection is created against the first host. It then manually sets the 2nd node as down (even
        /// though it is still up), verifying that the driver see that host 2 as down. Finally it waits 5 seconds for the 
        /// timer-based reconnection policy to kick in, and verifies that the driver sees the 2nd host as back up.
        ///
        /// @since 2.7.0
        /// @jira_ticket CSHARP-280
        /// @expected_result Host 2 should be automatically reconnected in the background after being set as down
        ///
        /// @test_assumptions
        ///    - A Cassandra cluster with 2 nodes
        /// @test_category connection:reconnection
        [Test]
        public void Reconnection_Attempts_Are_Made_In_The_Background()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);

            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithPoolingOptions(
                                     new PoolingOptions()
                                         .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                                         .SetHeartBeatInterval(0))
                                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(2000))
                                 .Build();
            var session = (Session)cluster.Connect();
            TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 10);
            var host = cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 2);
            // Check that the control connection is connected to another host
            Assert.AreNotEqual(cluster.Metadata.ControlConnection.Address, host.Address);
            Assert.True(host.IsUp);
            Trace.TraceInformation("Setting host 2 of 2 as down");
            host.SetDown();
            Assert.False(host.IsUp);
            Trace.TraceInformation("Waiting for 5 seconds");
            Thread.Sleep(5000);
            Assert.True(host.IsUp);
        }

        /// Tests that reconnection attempts are made multiple times in the background
        ///
        /// Reconnection_Attempted_Multiple_Times tests that the driver automatically reschedules host reconnections using 
        /// timers in the background multiple times. It first creates a Cassandra cluster with a single node, and verifies
        /// that the driver considers it as up. It then stops the single node and verifies that the driver considers the node
        /// as down, by both executing a query and retreiving a NoHostAvailableException, and checking the host status manually.
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);

            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .WithPoolingOptions(
                                            new PoolingOptions()
                                                .SetCoreConnectionsPerHost(HostDistance.Local, 2))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(2000))
                                        .Build())
            {
                var session = (Session)cluster.Connect();
                TestHelper.Invoke(() => session.Execute("SELECT * FROM system.local"), 10);
                var host = cluster.AllHosts().First();
                Assert.True(host.IsUp);
                Trace.TraceInformation("Stopping node 1 of 1");
                testCluster.Stop(1);
                // Make sure the node is considered down
                Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT * FROM system.local"));
                Assert.False(host.IsUp);
                Trace.TraceInformation("Waiting for 15 seconds");
                Thread.Sleep(15000);
                Assert.False(host.IsUp);
                Trace.TraceInformation("Restarting node 1 of 1");
                testCluster.Start(1);
                Trace.TraceInformation("Waiting for 5 seconds");
                Thread.Sleep(5000);
                Assert.True(host.IsUp);
            }
        }

        [Test]
        public void Cluster_Should_Ignore_IpV6_Addresses_For_Not_Valid_Hosts()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 0, true, false);
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(IPAddress.Parse("::1"))
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .Build())
            {
                Assert.DoesNotThrow(() =>
                {
                    var session = cluster.Connect();
                    session.Execute("select * from system.local");
                });
            }
        }

        /// <summary>
        /// Tests that when a peer is added or set as down, the address translator is invoked
        /// </summary>
        [Test]
        public void AddressTranslatorIsCalledPerEachPeer()
        {
            var invokedEndPoints = new List<IPEndPoint>();
            var translatorMock = new Mock<IAddressTranslator>(MockBehavior.Strict);
            translatorMock
                .Setup(t => t.Translate(It.IsAny<IPEndPoint>()))
                .Callback<IPEndPoint>(invokedEndPoints.Add)
                .Returns<IPEndPoint>(e => e);
            var testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(int.MaxValue))
                .WithAddressTranslator(translatorMock.Object)
                .Build();
            cluster.Connect();
            //2 peers translated
            Assert.AreEqual(2, invokedEndPoints.Count);
            Assert.True(cluster.AllHosts().All(h => h.IsUp));
            testCluster.Stop(3);
            //Wait for the C* event to notify the control connection
            Thread.Sleep(30000);
            //Should be down
            Assert.False(cluster.AllHosts().First(h => TestHelper.GetLastAddressByte(h) == 3).IsUp);
            
            //Should have been translated
            Assert.AreEqual(3, invokedEndPoints.Count);
            //The recently translated should be the host #3
            Assert.AreEqual(3, TestHelper.GetLastAddressByte(invokedEndPoints.Last()));
            cluster.Dispose();
        }

        /// <summary>
        /// Tests that a node is down and the schema is not the same, it waits until the max wait time is reached
        /// </summary>
        [Test]
        public void ClusterWaitsForSchemaChangesUntilMaxWaitTimeIsReached()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2, 0, true, false);
            nonShareableTestCluster.Stop(2);
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(nonShareableTestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                //Will wait for all the nodes to have the same schema
                session.Execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
            }
        }

        /// <summary>
        /// Tests that a node is down and the schema is not the same, it waits until the max wait time is reached
        /// </summary>
        [Test]
        public void ClusterWaitsForSchemaChangesUntilMaxWaitTimeIsReachedMultiple()
        {
            var index = 0;
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2, 0, true, false);
            nonShareableTestCluster.Stop(2);
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(nonShareableTestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                //Will wait for all the nodes to have the same schema
                session.Execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
                session.ChangeKeyspace("ks1");
                TestHelper.ParallelInvoke(() =>
                {
                    session.Execute("CREATE TABLE tbl1" + Interlocked.Increment(ref index) + " (id uuid primary key)");
                }, 10);
            }
        }
    }
}
