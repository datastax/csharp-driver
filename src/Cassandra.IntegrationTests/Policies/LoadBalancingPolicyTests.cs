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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies
{
    [TestFixture, Category("long")]
    public class LoadBalancingPolicyTests : TestGlobals
    {
        private static readonly Logger logger = new Logger(typeof(LoadBalancingPolicyTests));
        private TraceLevel _originalTraceLevel;
        private PolicyTestTools _policyTestTools = null;

        [SetUp]
        public void TestSetup()
        {
            _policyTestTools = new PolicyTestTools();
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
        public void PoliciesAreDifferentInstancesWhenDefault()
        {
            var builder = Cluster.Builder();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2, 2, DefaultMaxClusterCmdRetries, true);

            using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", testCluster.ClusterIpPrefix)).Build())
            using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", testCluster.ClusterIpPrefix)).Build())
            {
                using (var session1 = (Session)cluster1.Connect())
                using (var session2 = (Session)cluster2.Connect())
                {
                    Assert.True(!Object.ReferenceEquals(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy), "Load balancing policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy), "Reconnection policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy), "Retry policy instances should be different");
                }
            }
        }

        [Test]
        public void RoundRobin_NewNodeAddedToSameDc()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 1);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 6);

            // Add another node to the same DC
            _policyTestTools.ResetCoordinators();
            testCluster.BootstrapNode(3);
            TestUtils.WaitFor(testCluster.ClusterIpPrefix + "3", testCluster.Cluster, 60);
            string newlyBootstrappedHost = testCluster.ClusterIpPrefix + 3;
            TestUtils.ValidateBootStrappedNodeIsQueried(testCluster, 3, newlyBootstrappedHost);

            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 4);

            // decommission old node
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(1);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 60);

            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 6);
        }

        [Test]
        public void RoundRobin_TwoDCs_OneNodeDecommissioned()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2, 2, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            // Validate that all host were queried equally
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 3);

            // Add new node to the end of second cluster, remove node from beginning of first cluster
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(1);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 20);

            // Validate expected nodes where queried
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 4);

        }

        [Test]
        public void RoundRobin_OneDc_OneNodeBootStrapped()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            // Validate that all host were queried equally
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 12);

            // Add new node to the end of second cluster, remove node from beginning of first cluster
            _policyTestTools.ResetCoordinators();
            // Bootstrap step
            int bootStrapPos = 2;
            testCluster.BootstrapNode(bootStrapPos);
            string newlyBootstrappedIp = testCluster.ClusterIpPrefix + bootStrapPos;
            TestUtils.WaitForUp(newlyBootstrappedIp, testCluster.Builder, 40);

            // Validate expected nodes where queried
            _policyTestTools.WaitForPolicyToolsQueryToHitBootstrappedIp(testCluster, newlyBootstrappedIp);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "1", 6);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "2", 6);

        }

        [Test]
        public void RoundRobin_TwoDCs_OneNodeBootStrapped()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            // Validate that all host were queried equally
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 6);

            // Add new node to the end of second cluster, remove node from beginning of first cluster
            _policyTestTools.ResetCoordinators();
            // Bootstrap step
            testCluster.BootstrapNode(3, "dc2");
            string newlyBootstrappedIp = testCluster.ClusterIpPrefix + "3";
            TestUtils.WaitForUp(newlyBootstrappedIp, testCluster.Builder, 40);

            // Validate expected nodes where queried
            _policyTestTools.WaitForPolicyToolsQueryToHitBootstrappedIp(testCluster, newlyBootstrappedIp);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "1", 4);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "2", 4);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "3", 4);

        }

        [Test]
        public void RoundRobin_DcAware_NonExistentDc()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            try
            {
                testCluster.InitClient();
                Assert.Fail("Expecte exception was not thrown!");
            }
            catch (ArgumentException e)
            {
                string expectedErrMsg = "Datacenter dc2 does not match any of the nodes, available datacenters: datacenter1.";
                Assert.IsTrue(e.Message.Contains(expectedErrMsg));
            }
        }

        [Test]
        public void RoundRobin_TwoDcs_DcAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2, 2, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            testCluster.InitClient();

            _policyTestTools.CreateMultiDcSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 6);
        }

        [Test]
        public void RoundRobin_OneDc_ForceStop()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(4);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new RoundRobinPolicy())
                                         .WithQueryTimeout(10000);
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 3);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 3);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(1);
            testCluster.StopForce(2);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 20);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 20);

            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);
            Assert.IsTrue(_policyTestTools.Coordinators[testCluster.ClusterIpPrefix + "3"] >= 3, 
                "query count should have been greater than or equal to 3 for node " + testCluster.ClusterIpPrefix + "3");
            Assert.IsTrue(_policyTestTools.Coordinators[testCluster.ClusterIpPrefix + "4"] >= 3,
                "query count should have been greater than or equal to 3 for node " + testCluster.ClusterIpPrefix + "4");

            testCluster.StopForce(3);
            testCluster.StopForce(4);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "3", testCluster.Cluster, 20);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "4", testCluster.Cluster, 20);

            try
            {
                _policyTestTools.Query(testCluster, 3);
                Assert.Fail("Exception should have been thrown, but wasn't!");
            }
            catch (NoHostAvailableException)
            {
                logger.Info("Expected NoHostAvailableException exception was thrown.");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void DcAwareRoundRobin_TwoDCs_FourNodesDecommisionedOneAtATime()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2, 2, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
            testCluster.InitClient();

            _policyTestTools.CreateMultiDcSchema(testCluster.Session);
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 6);

            // Decommission node from current DC
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(3);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "3", testCluster.Cluster, 30);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 12);

            // Decommission final node from current DC
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(4);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "4", testCluster.Cluster, 30);
                _policyTestTools.Query(testCluster, 12);
            if (_policyTestTools.Coordinators.ContainsKey(testCluster.ClusterIpPrefix + "1"))
                _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 12);
            else
                _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 0);

            // Decommission node from recently updated DC
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(1);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 30);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 0);

            // Stop final node
            // Decommission node from recently updated DC
            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(2);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 30);

            try
            {
                _policyTestTools.Query(testCluster, 3);
            }
            catch (NoHostAvailableException)
            {
                logger.Info("Expected NoHostAvailableException exception was thrown.");
                return;
            }
            Assert.Fail("Expected exception was not thrown!");
        }

        [Test]
        public void TokenAwareTargetsPartitionNoHopsQuery()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 1);
            var traces = new List<QueryTrace>();
            for (var i = -10; i < 10; i++)
            {
                var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + _policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                    .SetRoutingKey(new RoutingKey() {RawRoutingKey = partitionKey})
                    .EnableTracing();
                var rs = testCluster.Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test, TestCassandraVersion(2, 0)]
        public void TokenAwareTargetsPartitionBindStringNoHopsQuery()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(4);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, PolicyTestTools.DefaultKeyspace, 1)));
            session.ChangeKeyspace(PolicyTestTools.DefaultKeyspace);
            session.WaitForSchemaAgreement(session.Execute(String.Format("CREATE TABLE {0} (k text PRIMARY KEY, i int)", _policyTestTools.TableName)));
            var traces = new List<QueryTrace>();
            string key = "value";
            for (var i = 100; i < 140; i++)
            {
                key += (char) i;
                var partitionKey = Encoding.UTF8.GetBytes(key);
                var statement = new SimpleStatement("INSERT INTO " + _policyTestTools.TableName + " (k, i) VALUES (?, ?)")
                    .Bind(key, i)
                    .SetRoutingKey(new RoutingKey() {RawRoutingKey = partitionKey})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionGuidNoHops()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(4);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, PolicyTestTools.DefaultKeyspace, 1));
            session.ChangeKeyspace(PolicyTestTools.DefaultKeyspace);
            session.Execute(String.Format("CREATE TABLE {0} (k uuid PRIMARY KEY, i int)", _policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var key = Guid.NewGuid();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + _policyTestTools.TableName + " (k, i) VALUES ({0}, {1})", key, i))
                    .SetRoutingKey(
                        new RoutingKey() {RawRoutingKey = TypeCodec.GuidShuffle(key.ToByteArray())})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionCompositeNoHops()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(4);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, PolicyTestTools.DefaultKeyspace, 1)));
            session.ChangeKeyspace(PolicyTestTools.DefaultKeyspace);
            session.WaitForSchemaAgreement(session.Execute(String.Format("CREATE TABLE {0} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))", _policyTestTools.TableName)));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = new SimpleStatement(String.Format("INSERT INTO " + _policyTestTools.TableName + " (k1, k2, i) VALUES ('{0}', {0}, {0})", i))
                    .SetRoutingKey(
                        new RoutingKey() {RawRoutingKey = Encoding.UTF8.GetBytes(i.ToString())},
                        new RoutingKey() {RawRoutingKey = BitConverter.GetBytes(i).Reverse().ToArray()})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionNoHopsPrepare()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            _policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            var pstmt = session.Prepare("INSERT INTO " + _policyTestTools.TableName + " (k, i) VALUES (?, ?)");
            for (var i = (int) short.MinValue; i < short.MinValue + 40; i++)
            {
                var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                var statement = pstmt
                    .Bind(i, i)
                    .SetRoutingKey(new RoutingKey() {RawRoutingKey = partitionKey})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAwareTargetsWrongPartitionHops()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder =
                Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            _policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(String.Format("INSERT INTO " + _policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                    .SetRoutingKey(new RoutingKey() {RawRoutingKey = new byte[] {0, 0, 0, 0}})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
        }

        [Test]
        public void DcAwareNeverHitsRemoteDc()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2, 2, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder =
                Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc1"));
            testCluster.InitClient();

            var hosts = testCluster.Cluster.AllHosts();
            //2 hosts in each datacenter
            Assert.AreEqual(2, hosts.Count(h => h.Datacenter == "dc1"));
            Assert.AreEqual(2, hosts.Count(h => h.Datacenter == "dc2"));
            var session = testCluster.Session;
            var queriedHosts = new List<IPAddress>();
            for (var i = 0; i < 50; i++)
            {
                var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                queriedHosts.Add(rs.Info.QueriedHost);
            }
            //Only the ones in the local
            Assert.True(queriedHosts.Contains(IPAddress.Parse(testCluster.ClusterIpPrefix + "1")), "Only hosts from the local DC should be queried 1");
            Assert.True(queriedHosts.Contains(IPAddress.Parse(testCluster.ClusterIpPrefix + "2")), "Only hosts from the local DC should be queried 2");
            Assert.False(queriedHosts.Contains(IPAddress.Parse(testCluster.ClusterIpPrefix + "3")), "Only hosts from the local DC should be queried 3");
            Assert.False(queriedHosts.Contains(IPAddress.Parse(testCluster.ClusterIpPrefix + "4")), "Only hosts from the local DC should be queried 4");
        }

        [Test]
        public void DcAwareWithWrongDatacenterNameThrows()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder =
                Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc1"));
            testCluster.InitClient();

            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.ClusterIpPrefix + "1")
                                 .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("wrong_dc"))
                                 .Build();
            var ex1 = Assert.Throws<ArgumentException>(() => cluster.Connect());
            // Keeps throwing the same exception
            var ex2 = Assert.Throws<ArgumentException>(() => cluster.Connect());
            Assert.True(Object.ReferenceEquals(ex1, ex2));
        }

        [Test]
        public void TokenAware()
        {
            TokenAwareTest(false);
        }

        [Test]
        public void TokenAwarePrepared()
        {
            TokenAwareTest(true);
        }

        public void TokenAwareTest(bool usePrepared)
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session);
            //clusterInfo.Cluster.RefreshSchema();
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            // Not the best test ever, we should use OPP and check we do it the
            // right nodes. But since M3P is hard-coded for now, let just check
            // we just hit only one node.
            int nodePosToDecommission = 2;
            int nodePositionToNotDecommission = 1;
            if (_policyTestTools.Coordinators.ContainsKey(testCluster.ClusterIpPrefix + "1"))
            {
                nodePosToDecommission = 1;
                nodePositionToNotDecommission = 2;
            }
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePosToDecommission, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePositionToNotDecommission, 0);

            // now try again having stopped the node that was just queried
            _policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(nodePosToDecommission);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + nodePosToDecommission, testCluster.Cluster, 40);
            _policyTestTools.Query(testCluster, 12, usePrepared);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePosToDecommission, 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePositionToNotDecommission, 12);
        }

        [Test]
        public void TokenAware_TwoDCsWithOneNodeEach_ReplicationFactorTwo()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder =
                Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 2);

            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 6);
        }
    }
}
