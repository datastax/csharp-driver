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

﻿using NUnit.Framework;
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
    public class PoolTests
    {
        protected TraceLevel _originalTraceLevel;

        protected virtual string IpPrefix
        {
            get
            {
                return Options.Default.IP_PREFIX;
            }
        }

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
            var builder = Cluster.Builder().WithReconnectionPolicy(policy);
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            try
            {
                var session = (Session)clusterInfo.Session;
                var hosts = new List<IPAddress>();
                for (var i = 0; i < 50; i++)
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    if (i == 20)
                    {
                        TestUtils.CcmStopForce(clusterInfo, 2);
                    }
                    else if (i == 30)
                    {
                        TestUtils.CcmStart(clusterInfo, 2);
                        Thread.Sleep(5000);
                    }
                    hosts.Add(rs.Info.QueriedHost);
                }

                var pool = session.GetConnectionPool(new Host(IPAddress.Parse(IpPrefix + "1"), policy), HostDistance.Local);
                var connections = pool.OpenConnections.ToArray();
                var expectedCoreConnections = clusterInfo.Cluster.Configuration
                    .GetPoolingOptions(connections.First().ProtocolVersion)
                    .GetCoreConnectionsPerHost(HostDistance.Local);
                Assert.AreEqual(expectedCoreConnections, connections.Length);
                Assert.True(connections.All(c => !c.IsClosed));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        /// Executes statements in parallel while killing nodes.
        /// </summary>
        [Test]
        public void FailoverTest()
        {
            var parallelOptions = new ParallelOptions();
            parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
            parallelOptions.MaxDegreeOfParallelism = 1000;

            var policy = new ConstantReconnectionPolicy(Int32.MaxValue);
            var builder = Cluster.Builder().WithReconnectionPolicy(policy);
            var clusterInfo = TestUtils.CcmSetup(4, builder);
            try
            {
                var session = clusterInfo.Session;
                Action selectAction = () =>
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    Assert.Greater(rs.Count(), 0);
                };

                var actions = new List<Action>();
                for (var i = 0; i < 100; i++ )
                {
                    actions.Add(selectAction);
                }

                //kill some nodes.
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 1));
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 2));
                actions.Insert(80, () => TestUtils.CcmStopForce(clusterInfo, 3));

                //Execute in parallel more than 100 actions
                Parallel.Invoke(parallelOptions, actions.ToArray());

                //Wait for it to be killed
                Thread.Sleep(30000);

                //Execute serially selects
                for (var i = 0; i < 100; i++)
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    Assert.Greater(rs.Count(), 0);
                    Assert.AreEqual(IpPrefix + "4", rs.Info.QueriedHost.ToString());
                }
                //The control connection should be using the available node
                StringAssert.StartsWith(IpPrefix + "4", clusterInfo.Cluster.Metadata.ControlConnection.BindAddress.ToString());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        /// Executes statements in parallel while killing nodes and restarting them.
        /// </summary>
        [Test]
        public void FailoverReconnectTest()
        {
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(), 
                MaxDegreeOfParallelism = 1000
            };

            var policy = new ConstantReconnectionPolicy(5000);
            var builder = Cluster.Builder().WithReconnectionPolicy(policy);
            var clusterInfo = TestUtils.CcmSetup(4, builder);
            try
            {
                var session = clusterInfo.Session;
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
                StringAssert.StartsWith(IpPrefix + "1", clusterInfo.Cluster.Metadata.ControlConnection.BindAddress.ToString());

                //Kill some nodes
                //Including the one used by the control connection
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 1));
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 2));
                actions.Insert(80, () => TestUtils.CcmStopForce(clusterInfo, 3));

                //Execute in parallel more than 100 actions
                Trace.TraceInformation("Start invoking with kill nodes");
                Parallel.Invoke(parallelOptions, actions.ToArray());

                //Wait for the nodes to be killed
                Thread.Sleep(30000);

                actions = new List<Action>();
                for (var i = 0; i < 100; i++)
                {
                    actions.Add(selectAction);
                }

                //Check that the control connection is using first host
                //bring back some nodes
                actions.Insert(3, () => TestUtils.CcmStart(clusterInfo, 3));
                actions.Insert(50, () => TestUtils.CcmStart(clusterInfo, 2));
                actions.Insert(50, () => TestUtils.CcmStart(clusterInfo, 1));

                //Execute in parallel more than 100 actions
                Trace.TraceInformation("Start invoking with restart nodes");
                Parallel.Invoke(parallelOptions, actions.ToArray());


                //Wait for the nodes to be restarted
                Thread.Sleep(30000);

                var queriedHosts = new List<IPAddress>();
                for (var i = 0; i < 100; i++)
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    queriedHosts.Add(rs.Info.QueriedHost);
                }
                //Check that one of the restarted nodes were queried
                Assert.True(queriedHosts.Any(address => address.ToString().StartsWith(IpPrefix + "3")));
                //Check that the control connection is still using last host
                StringAssert.StartsWith(IpPrefix + "4", clusterInfo.Cluster.Metadata.ControlConnection.BindAddress.ToString());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        /// Tests that the pool behaves when adding a node and the driver queries the node after is bootstrapped
        /// </summary>
        [Test]
        public void BootstrapNodeTest()
        {
            var policy = new ConstantReconnectionPolicy(500);
            var builder = Cluster.Builder().WithReconnectionPolicy(policy);
            var clusterInfo = TestUtils.CcmSetup(1, builder);
            try
            {
                var session = clusterInfo.Session;
                for (var i = 0; i < 100; i++)
                {
                    if (i == 50)
                    {
                        TestUtils.CcmBootstrapNode(clusterInfo, 2);
                        TestUtils.CcmStart(clusterInfo, 2);
                    }
                    session.Execute("SELECT * FROM system.schema_columnfamilies").Count();
                }
                //Wait for the join to be online
                Thread.Sleep(120000);
                var list = new List<IPAddress>();
                for (var i = 0; i < 100; i++)
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    rs.Count();
                    list.Add(rs.Info.QueriedHost);
                }
                Assert.That(clusterInfo.Cluster.Metadata.AllHosts().ToList().Count, Is.EqualTo(2));
                Assert.True(list.Any(ip => ip.ToString() == IpPrefix + "2"), "The new node should be queried");
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void InitialKeyspaceRaceTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);

            try
            {
                var cluster = Cluster.Builder()
                    .AddContactPoint(IpPrefix + "1")
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void ConnectWithWrongKeyspaceNameTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);

            try
            {
                var cluster = Cluster.Builder()
                    .AddContactPoint(IpPrefix + "1")
                    //using a keyspace that does not exists
                    .WithDefaultKeyspace("DOES_NOT_EXISTS")
                    .Build();

                var ex = Assert.Throws<InvalidQueryException>(() => cluster.Connect());
                Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES"));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void ConnectShouldResolveNames()
        {
            var clusterInfo = TestUtils.CcmSetup(1);

            try
            {
                var cluster = Cluster.Builder()
                    .AddContactPoint("localhost")
                    .Build();

                var session = cluster.Connect("system");
                StringAssert.StartsWith(IpPrefix + "1", cluster.AllHosts().First().Address.ToString());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
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
            var clusterInfo = TestUtils.CcmSetup(1);
            var session = clusterInfo.Session;
            //For a node to be back up could take up to 60 seconds
            const int bringUpNodeMilliseconds = 60000;
            try
            {
                Action dropConnections = () =>
                {
                    session.Execute("SELECT * FROM system.schema_keyspaces");
                    TestUtils.CcmStopForce(clusterInfo, 1);
                    Thread.Sleep(2000);
                    TestUtils.CcmStart(clusterInfo, 1);
                };
                Action query = () =>
                {
                    Thread.Sleep(bringUpNodeMilliseconds);
                    //All the nodes should be up but the socket connections are not valid
                    session.Execute("SELECT * FROM system.schema_keyspaces");
                };
                Parallel.Invoke(parallelOptions, dropConnections, query);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void HeartbeatShouldDetectNodeDown()
        {
            //Execute a couple of time
            //Kill connections the node silently
            //Do nothing for a while
            //Check if the node is considered as down
            var clusterInfo = TestUtils.CcmSetup(1);

            try
            {
                var cluster = Cluster.Builder()
                    .AddContactPoint(IpPrefix + "1")
                    .WithPoolingOptions(
                        new PoolingOptions()
                        .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                        .SetHeartBeatInterval(500))
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(Int32.MaxValue))
                    .Build();
                var session = (Session)cluster.Connect();
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
