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

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class PoolTests : TestGlobals
    {
        protected TraceLevel _originalTraceLevel;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Diagnostics.CassandraTraceSwitch.Level = _originalTraceLevel;
        }

        [Test]
        public void ReconnectionRecyclesPool()
        {
            var policy = new ConstantReconnectionPolicy(5000);

            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2);
            nonShareableTestCluster.Builder = new Builder().WithReconnectionPolicy(policy);
            nonShareableTestCluster.InitClient(); // this will replace the existing session using the newly assigned Builder instance
            var session = (Session)nonShareableTestCluster.Session;

            var hosts = new List<IPAddress>();
            for (var i = 0; i < 50; i++)
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
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

            var pool = session.GetConnectionPool(new Host(IPAddress.Parse(nonShareableTestCluster.InitialContactPoint), policy), HostDistance.Local);
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

            var policy = new ConstantReconnectionPolicy(Int32.MaxValue);
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(4);
            nonShareableTestCluster.Builder = new Builder().WithReconnectionPolicy(policy);
            nonShareableTestCluster.InitClient(); // this will replace the existing session using the newly assigned Builder instance
            var session = nonShareableTestCluster.Session;

            // Check query to host distribution before killing nodes
            var queriedHosts = new List<string>();
            DateTime futureDateTime = DateTime.Now.AddSeconds(120);
            while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                queriedHosts.Add(rs.Info.QueriedHost.ToString());
                Thread.Sleep(50);
            }
            Assert.AreEqual(4, (from singleHost in queriedHosts select singleHost).Distinct().Count(), "All hosts should have been queried!");

            // Create List of actions
            Action selectAction = () =>
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
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
                var rowSet2 = session.Execute("SELECT * FROM system.schema_columnfamilies");
                Assert.Greater(rowSet2.Count(), 0);
                Assert.AreEqual(nonShareableTestCluster.ClusterIpPrefix + "4", rowSet2.Info.QueriedHost.ToString());
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
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(4);
            nonShareableTestCluster.Builder = new Builder().WithReconnectionPolicy(policy);
            nonShareableTestCluster.InitClient(); // this will replace the existing session using the newly assigned Builder instance
            var session = nonShareableTestCluster.Session;

            // Check query to host distribution before killing nodes
            var queriedHosts = new List<string>();
            DateTime futureDateTime = DateTime.Now.AddSeconds(120);
            while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                queriedHosts.Add(rs.Info.QueriedHost.ToString());
                Thread.Sleep(50);
            }
            Assert.AreEqual(4, (from singleHost in queriedHosts select singleHost).Distinct().Count(), "All hosts should have been queried!");

            // Create list of actions
            Action selectAction = () =>
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                Assert.Greater(rs.Count(), 0);
            };
            var actions = new List<Action>();
            for (var i = 0; i < 100; i++)
            {
                actions.Add(selectAction);
            }

            //Check that the control connection is using first host
            StringAssert.StartsWith(nonShareableTestCluster.ClusterIpPrefix + "1", nonShareableTestCluster.Cluster.Metadata.ControlConnection.BindAddress.ToString());

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
            for (var i = 0; i < 100; i++)
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
            TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "1", nonShareableTestCluster.Builder, 20);
            TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "2", nonShareableTestCluster.Builder, 20);
            TestUtils.WaitForUp(nonShareableTestCluster.ClusterIpPrefix + "3", nonShareableTestCluster.Builder, 20);

            queriedHosts.Clear();
            // keep querying hosts until they are all queried, or time runs out
            futureDateTime = DateTime.Now.AddSeconds(120);
            while ((from singleHost in queriedHosts select singleHost).Distinct().Count() < 4 && DateTime.Now < futureDateTime)
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                queriedHosts.Add(rs.Info.QueriedHost.ToString());
                Thread.Sleep(50);
            }
            //Check that one of the restarted nodes were queried
            Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "1", queriedHosts);
            Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "2", queriedHosts);
            Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "3", queriedHosts);
            Assert.Contains(nonShareableTestCluster.ClusterIpPrefix + "4", queriedHosts);
            //Check that the control connection is still using last host
            StringAssert.StartsWith(nonShareableTestCluster.ClusterIpPrefix + "4", nonShareableTestCluster.Cluster.Metadata.ControlConnection.BindAddress.ToString());
        }

        /// <summary>
        /// Validates that the client adds the newly bootstrapped node and eventually queries from it
        /// </summary>
        [Test]
        public void BootstrappedNode()
        {
            var policy = new ConstantReconnectionPolicy(500);
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);
            testCluster.Builder = new Builder().WithReconnectionPolicy(policy);
            testCluster.InitClient(); // this will replace the existing session using the newly assigned Builder instance
            testCluster.BootstrapNode(2);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", testCluster.Builder, 30);

            //Wait for the join to be online
            string newlyBootstrappedHost = testCluster.ClusterIpPrefix + 2;
            TestUtils.ValidateBootStrappedNodeIsQueried(testCluster, 2, newlyBootstrappedHost);
        }

        [Test]
        public void InitialKeyspaceRaceTest()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);

            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                //using a keyspace
                .WithDefaultKeyspace("system")
                //lots of connections per host
                .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 30))
                .Build();

            var actions = new List<Action>(1000);
            var session = cluster.Connect();
            //Try to be force a race condition
            for (var i = 0; i < 1000; i++)
            {
                //Some table within the system keyspace
                actions.Add(() =>
                {
                    var task = session.ExecuteAsync(new SimpleStatement("SELECT * FROM schema_columnfamilies"));
                    task.Wait();
                });
            }
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(),
                MaxDegreeOfParallelism = 1000
            };
            Parallel.Invoke(parallelOptions, actions.ToArray());
        }

        [Test]
        public void ConnectWithWrongKeyspaceNameTest()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);

            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                //using a keyspace that does not exists
                .WithDefaultKeyspace("DOES_NOT_EXISTS_" + Randomm.RandomAlphaNum(12))
                .Build();

            var ex = Assert.Throws<InvalidQueryException>(() => cluster.Connect());
            Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES"));
        }

        [Test]
        public void ConnectShouldResolveNames()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);

            var cluster = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .Build();

            var session = testCluster.Cluster.Connect("system");
            StringAssert.StartsWith(testCluster.InitialContactPoint, cluster.AllHosts().First().Address.ToString());
        }

        [Test]
        [Explicit("This test needs to be rebuilt, when restarting the Cassandra node on Windows new connections are refused")]
        public void DroppingConnectionsTest()
        {
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(),
                MaxDegreeOfParallelism = 1000
            };
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(1);
            var session = nonShareableTestCluster.Session;
            //For a node to be back up could take up to 60 seconds
            const int bringUpNodeMilliseconds = 60000;
            Action dropConnections = () =>
            {
                session.Execute("SELECT * FROM system.schema_keyspaces");
                nonShareableTestCluster.StopForce(1);
                Thread.Sleep(2000);
                nonShareableTestCluster.Start(1);
            };
            Action query = () =>
            {
                Thread.Sleep(bringUpNodeMilliseconds);
                //All the nodes should be up but the socket connections are not valid
                session.Execute("SELECT * FROM system.schema_keyspaces");
            };
            Parallel.Invoke(parallelOptions, dropConnections, query);
        }

        [Test]
        public void HeartbeatShouldDetectNodeDown()
        {
            //Execute a couple of time
            //Kill connections the node silently
            //Do nothing for a while
            //Check if the node is considered as down
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);

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
                session.Execute("SELECT * FROM system.schema_keyspaces");
            }
            var host = cluster.AllHosts().First();
            var pool = session.GetConnectionPool(host, HostDistance.Local);
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
                catch (NoHostAvailableException e)
                {
                }
            }

            //Now the node is ready to accept connections
            var session = cluster.Connect("system");
            TestHelper.ParallelInvoke(() => session.Execute("SELECT * from schema_keyspaces"), 20);
        }
    }
}
