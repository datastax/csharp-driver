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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class PoolTests : TestGlobals
    {
        protected TraceLevel OriginalTraceLevel;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            OriginalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            Diagnostics.CassandraTraceSwitch.Level = OriginalTraceLevel;
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
        /// Tests that the reconnection attempt (on a dead node) is attempted only once per try (when allowed by the reconnection policy).
        /// </summary>
        [Test]
        [TestTimeout(120000)]
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
            cluster.Metadata.Hosts.Down += h =>
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

        /// <summary>
        /// Tests that when a peer is added or set as down, the address translator is invoked
        /// </summary>
        [Test]
        public void AddressTranslatorIsCalledPerEachPeer()
        {
            var invokedEndPoints = new List<IPEndPoint>();
            var translatorMock = new Moq.Mock<IAddressTranslator>(Moq.MockBehavior.Strict);
            translatorMock
                .Setup(t => t.Translate(Moq.It.IsAny<IPEndPoint>()))
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
