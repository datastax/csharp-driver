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

            var policy = new ConstantReconnectionPolicy(300);
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

                actions = new List<Action>();
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
            var parallelOptions = new ParallelOptions();
            parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
            parallelOptions.MaxDegreeOfParallelism = 1000;

            var policy = new ConstantReconnectionPolicy(1000);
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

                //kill some nodes.
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 1));
                actions.Insert(20, () => TestUtils.CcmStopForce(clusterInfo, 2));
                actions.Insert(80, () => TestUtils.CcmStopForce(clusterInfo, 3));

                //Execute in parallel more than 100 actions
                Parallel.Invoke(parallelOptions, actions.ToArray());

                actions = new List<Action>();
                for (var i = 0; i < 100; i++)
                {
                    actions.Add(selectAction);
                }

                //bring back some nodes
                actions.Insert(3, () => TestUtils.CcmStart(clusterInfo, 3));
                actions.Insert(50, () => TestUtils.CcmStart(clusterInfo, 2));
                actions.Insert(50, () => TestUtils.CcmStart(clusterInfo, 1));

                //Execute in parallel more than 100 actions
                Parallel.Invoke(parallelOptions, actions.ToArray());
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
                Assert.True(list.Any(ip => ip.ToString() == IpPrefix + "2"), "The new node should be queried");
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
