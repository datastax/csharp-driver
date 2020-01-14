//
//      Copyright (C) DataStax Inc.
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
using System.Threading;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class LoadBalancingPolicyTests : TestGlobals
    {

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

        /// Tests that the TokenMap can be rebulit using an existing keyspace's RF, even with a decommisioned DC
        ///
        /// TokenMap_Rebuild_With_Decommissioned_DC_Existing_RF tests that a keyspace with a non-existent DC in the replication
        /// options can be connected. It first creates a 2 datacenter cluster with 1 node in each. It then connects to this cluster
        /// using dc1 and creates a multi-dc schema which has a replication factor of 1 in each dc. It performs a simple insertion
        /// and verifies that the reads are performed from dc1 as expected. It then decommissions dc1 by removing its only node, causing
        /// the keyspace replication factor to point to a non-existing "dc1". It then verifies that the driver is able to reconnect to the 
        /// cluster via dc2. Finally it performs some simple reads to verify that the data is read from dc2.
        ///
        /// @since 3.0.1
        /// @jira_ticket CSHARP-385
        /// @expected_result TokenMap is successfully rebuilt with decomissioned DC, with existing RFs.
        ///
        /// @test_category control_connection
        [Test]
        public void TokenMap_Rebuild_With_Decommissioned_DC_Existing_RF()
        {
            // Create a 2dc:1node each cluster
            var clusterOptions = new TestClusterOptions();
            clusterOptions.Dc2NodeLength = 1;
            var testCluster = TestClusterManager.CreateNew(1, clusterOptions);

            testCluster.Builder = Cluster.Builder().AddContactPoint("127.0.0.1").WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc1"));
            testCluster.Cluster = testCluster.Builder.Build();
            testCluster.Session = testCluster.Cluster.Connect();

            PolicyTestTools policyTestTools = new PolicyTestTools();
            // Create a ks with RF = dc1:1, dc2:1
            policyTestTools.CreateMultiDcSchema(testCluster.Session, 1, 1);
            policyTestTools.InitPreparedStatement(testCluster, 12, false, ConsistencyLevel.All);
            // Replicas now in each node in each dc

            policyTestTools.Query(testCluster, 12, false);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 12);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 0);
            testCluster.Cluster.Shutdown();

            testCluster.DecommissionNode(1);
            // dc1 no longer has any hosts

            testCluster.Builder = Cluster.Builder().AddContactPoint("127.0.0.2").WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            testCluster.Cluster = testCluster.Builder.Build();
            // Should be able to connect and rebuild token map
            testCluster.Session = testCluster.Cluster.Connect(policyTestTools.DefaultKeyspace);

            policyTestTools.ResetCoordinators();
            policyTestTools.Query(testCluster, 12, false);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 12);
            testCluster.Cluster.Shutdown();
        }
    }
}
