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
using System.Text;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("long")]
    public class LoadBalancingPolicyTests : TestGlobals
    {
        /// <summary>
        /// Validate that two sessions connected to the same DC use separate Policy instances
        /// </summary>
        [Test]
        public void TwoSessionsConnectedToSameDcUseSeparatePolicyInstances()
        {
            var builder = Cluster.Builder();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCreateRetries, true);

            using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", testCluster.ClusterIpPrefix)).Build())
            using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", testCluster.ClusterIpPrefix)).Build())
            {
                using (var session1 = (Session)cluster1.Connect())
                using (var session2 = (Session)cluster2.Connect())
                {
                    Assert.True(!ReferenceEquals(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy), "Load balancing policy instances should be different");
                    Assert.True(!ReferenceEquals(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy), "Reconnection policy instances should be different");
                    Assert.True(!ReferenceEquals(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy), "Retry policy instances should be different");
                }
            }
        }

        /// <summary>
        /// Using a default round robin load balancing policy, connected to a cluster with one DC,
        /// validate that the session behaves as expected when a node is added and then another is decommissioned.
        /// 
        /// @test_category consistency
        /// @test_category connection:outage
        /// @test_category load_balancing:round_robin
        /// </summary>
        [Test]
        public void RoundRobin_OneDc_OneNodeAdded_OneNodeDecommissioned()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            policyTestTools.CreateSchema(testCluster.Session);
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            // Validate that all host were queried equally
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 12);

            // Add new node to the end of second cluster, remove node from beginning of first cluster
            policyTestTools.ResetCoordinators();
            // Bootstrap step
            int bootStrapPos = 2;
            testCluster.BootstrapNode(bootStrapPos);
            string newlyBootstrappedIp = testCluster.ClusterIpPrefix + bootStrapPos;
            TestUtils.WaitForUp(newlyBootstrappedIp, DefaultCassandraPort, 30);

            // Validate expected nodes where queried
            policyTestTools.WaitForPolicyToolsQueryToHitBootstrappedIp(testCluster, newlyBootstrappedIp);
            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 6);

            // decommission old node
            policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(1);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, testCluster.Cluster, 60);

            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 12);
        }

        /// <summary>
        /// Using a default round robin load balancing policy, connected to a cluster with multiple DCs,
        /// validate that the session behaves as expected when a node is added and then another is decommissioned from each DC.
        /// 
        /// @test_category consistency
        /// @test_category connection:outage
        /// @test_category load_balancing:round_robin
        /// </summary>
        [Test]
        public void RoundRobin_TwoDCs_EachDcHasOneNodeAddedAndDecommissioned()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCreateRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            testCluster.InitClient();

            policyTestTools.CreateSchema(testCluster.Session);
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            // Validate that all host were queried equally
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 6);

            // Add new node to the end of first cluster, remove node from beginning of first cluster
            policyTestTools.ResetCoordinators();
            // Bootstrap step
            testCluster.BootstrapNode(3, "dc1");
            string newlyBootstrappedIp = testCluster.ClusterIpPrefix + "3";
            TestUtils.WaitForUp(newlyBootstrappedIp, DefaultCassandraPort, 30);

            // Validate expected nodes where queried
            policyTestTools.WaitForPolicyToolsQueryToHitBootstrappedIp(testCluster, newlyBootstrappedIp);
            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 4);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 4);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, 4);

            // Remove node from beginning of first cluster
            policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(1);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, testCluster.Cluster, 20);

            // Validate expected nodes where queried
            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, 6);

            // Add new node to the end of second cluster, remove node from beginning of first cluster
            policyTestTools.ResetCoordinators();
            testCluster.BootstrapNode(4, "dc2");
            newlyBootstrappedIp = testCluster.ClusterIpPrefix + "4";
            TestUtils.WaitForUp(newlyBootstrappedIp, DefaultCassandraPort, 30);
            policyTestTools.ResetCoordinators();
            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 4);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, 4);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4:" + DefaultCassandraPort, 4);

            // Remove node from beginning of second cluster
            policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(2);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 20);
            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4:" + DefaultCassandraPort, 6);
        }

        /// <summary>
        /// Validated that the driver fails as expected with a non-existing datacenter is specified via DCAwareRoundRobinPolicy 
        /// 
        /// @test_category load_balancing:dc_aware
        /// </summary>
        [Test]
        public void RoundRobin_DcAware_BuildClusterWithNonExistentDc()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            try
            {
                testCluster.InitClient();
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (ArgumentException e)
            {
                string expectedErrMsg = "Datacenter dc2 does not match any of the nodes, available datacenters: datacenter1.";
                Assert.IsTrue(e.Message.Contains(expectedErrMsg));
            }
        }

        /// <summary>
        /// Validated that the driver only uses the DC specified by DCAwareRoundRobinPolicy 
        /// 
        /// @test_category load_balancing:dc_aware
        /// </summary>
        [Test]
        public void RoundRobin_TwoDCs_DcAware()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCreateRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            testCluster.InitClient();

            policyTestTools.CreateMultiDcSchema(testCluster.Session);
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 12);
        }

        /// <summary>
        /// Validate that the driver behaves as expected as every node of a single DC cluster is force-stopped
        /// 
        /// @test_category connection:outage
        /// @test_category load_balancing:dc_aware
        /// </summary>
        [Test]
        public void RoundRobin_OneDc_AllNodesForceStoppedOneAtATime()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new RoundRobinPolicy())
                                         .WithQueryTimeout(10000);
            testCluster.InitClient();

            policyTestTools.CreateSchema(testCluster.Session);
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 6);

            policyTestTools.ResetCoordinators();
            testCluster.StopForce(1);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 20);

            policyTestTools.Query(testCluster, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 12);

            testCluster.StopForce(2);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 20);

            try
            {
                policyTestTools.Query(testCluster, 3);
                Assert.Fail("Exception should have been thrown, but wasn't!");
            }
            catch (NoHostAvailableException)
            {
                Trace.TraceInformation("Expected NoHostAvailableException exception was thrown.");
            }
        }

        /// <summary>
        /// Validate that the expected nodes are queried when using a TokenAware RoundRobin policy, 
        /// using two data centers, replication factor 2 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void RoundRobin_TokenAware_TwoDCsWithOneNodeEach_ReplicationFactorTwo()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCreateRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            policyTestTools.CreateSchema(testCluster.Session, 2);

            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 6);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 6);
        }

        /// <summary>
        /// Validate that no hops occur when inserting into a single partition 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_TargetPartition_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(testCluster.Session, 1);
            var traces = new List<QueryTrace>();
            for (var i = -10; i < 10; i++)
            {
                var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
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

        /// <summary>
        /// Validate that no hops occur when inserting GUID values into the key 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_Guid_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            string uniqueTableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session);
            session.Execute(String.Format("CREATE TABLE {0} (k uuid PRIMARY KEY, i int)", uniqueTableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var key = Guid.NewGuid();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + uniqueTableName + " (k, i) VALUES ({0}, {1})", key, i))
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

        /// <summary>
        /// Validate that no hops occur when inserting into a composite key 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_Composite_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.CreateSchema(session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            session.Execute(String.Format("CREATE TABLE {0} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))", policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k1, k2, i) VALUES ('{0}', {0}, {0})", i))
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

        /// <summary>
        /// Validate that no hops occur when inserting string values via a prepared statement 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// @test_category prepared_statements
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TokenAware_BindString_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.CreateSchema(session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            session.Execute(String.Format("CREATE TABLE {0} (k text PRIMARY KEY, i int)", policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            string key = "value";
            for (var i = 100; i < 140; i++)
            {
                key += (char)i;
                var partitionKey = Encoding.UTF8.GetBytes(key);
                var statement = new SimpleStatement("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES (?, ?)", key, i)
                    .SetRoutingKey(new RoutingKey() { RawRoutingKey = partitionKey })
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

        /// <summary>
        /// Validate that no hops occur when inserting int values via a prepared statement 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// @test_category prepared_statements
        /// </summary>
        [Test]
        public void TokenAware_BindInt_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            var pstmt = session.Prepare("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES (?, ?)");
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

        /// <summary>
        /// Validate that hops occur when the wrong partition is targeted 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_TargetWrongPartition_HopsOccur()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                    .SetRoutingKey(new RoutingKey() {RawRoutingKey = new byte[] {0, 0, 0, 0}})
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
        }

        /// <summary>
        /// Validate that the expected nodes are queried when using a TokenAware RoundRobin policy, 
        /// executing non-prepared statements 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// </summary>
        [Test]
        public void RoundRobin_TokenAware_NotPrepared()
        {
            TokenAwareTest(false);
        }

        /// <summary>
        /// Validate that the expected nodes are queried when using a TokenAware RoundRobin policy, 
        /// executing prepared statements 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// </summary>
        [Test]
        public void RoundRobin_TokenAware_Prepared()
        {
            TokenAwareTest(true);
        }

        public void TokenAwareTest(bool usePrepared)
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, DefaultMaxClusterCreateRetries, true);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            policyTestTools.CreateSchema(testCluster.Session);
            //clusterInfo.Cluster.RefreshSchema();
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);

            // Not the best test ever, we should use OPP and check we do it the
            // right nodes. But since M3P is hard-coded for now, let just check
            // we just hit only one node.
            int nodePosToDecommission = 2;
            int nodePositionToNotDecommission = 1;
            if (policyTestTools.Coordinators.ContainsKey(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort))
            {
                nodePosToDecommission = 1;
                nodePositionToNotDecommission = 2;
            }
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePosToDecommission + ":" + DefaultCassandraPort, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePositionToNotDecommission + ":" + DefaultCassandraPort, 0);

            // now try again having stopped the node that was just queried
            policyTestTools.ResetCoordinators();
            testCluster.DecommissionNode(nodePosToDecommission);
            TestUtils.waitForDecommission(testCluster.ClusterIpPrefix + nodePosToDecommission + ":" + DefaultCassandraPort, testCluster.Cluster, 40);
            policyTestTools.Query(testCluster, 12, usePrepared);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePosToDecommission + ":" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + nodePositionToNotDecommission + ":" + DefaultCassandraPort, 12);
        }

    }
}
