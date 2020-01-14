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
using System.Threading;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class RetryPolicyTests : TestGlobals
    {

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy
        /// </summary>
        [Test]
        public void RetryPolicy_DowngradingConsistency()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            DowngradingConsistencyRetryPolicyTest(builder);
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy with LoggingRetryPolicy
        /// 
        /// @test_category connection:retry_policy
        /// </summary>
        [Test]
        public void LoggingRetryPolicy_DowngradingConsistency()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.Instance));
            DowngradingConsistencyRetryPolicyTest(builder);
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy
        /// 
        /// @test_category connection:retry_policy
        /// </summary>
        public void DowngradingConsistencyRetryPolicyTest(Builder builder)
        {
            PolicyTestTools policyTestTools = new PolicyTestTools();

            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = builder;
            testCluster.InitClient();
            policyTestTools.CreateSchema(testCluster.Session, 3);

            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            TestUtils.WaitForSchemaAgreement(testCluster.Cluster);

            policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.All);

            policyTestTools.Query(testCluster, 12, ConsistencyLevel.All);
            policyTestTools.AssertAchievedConsistencyLevel(ConsistencyLevel.All);

            //Kill one node: 2 nodes alive
            testCluster.Stop(2);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 20);

            //After killing one node, the achieved consistency level should be downgraded
            policyTestTools.ResetCoordinators();
            policyTestTools.Query(testCluster, 12, ConsistencyLevel.All);
            policyTestTools.AssertAchievedConsistencyLevel(ConsistencyLevel.Two);
        }

        /// <summary>
        /// Test AlwaysIgnoreRetryPolicy with Logging enabled
        /// 
        /// @test_category connection:retry_policy,outage
        /// </summary>
        [Test]
        public void AlwaysIgnoreRetryPolicyTest()
        {

            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            testCluster.Builder = Cluster.Builder()
                                         .WithRetryPolicy(new LoggingRetryPolicy(AlwaysIgnoreRetryPolicy.Instance))
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "1")
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "2");
            testCluster.InitClient();
            RetryPolicyTest(testCluster);
        }

        /// <summary>
        /// Test AlwaysIgnoreRetryPolicy with Logging enabled
        /// 
        /// @test_category connection:retry_policy,outage
        /// </summary>
        [Test]
        public void AlwaysRetryRetryPolicyTest()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            testCluster.Builder = Cluster.Builder()
                                         .WithRetryPolicy(new LoggingRetryPolicy(AlwaysRetryRetryPolicy.Instance))
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "1")
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "2");
            testCluster.InitClient();
            RetryPolicyTest(testCluster);

        }

        /// Tests that retries are performed on the next host with useCurrentHost set to false
        ///
        /// TryNextHostRetryPolicyTest tests that the driver can use the next available node when retrying, instead of reusing
        /// the currently attempted node. This test uses a TryNextHostRetryPolicy that is defined with useCurrentHost set to false
        /// for each of read, write and unavailable exceptions, and a Cassandra cluster with 2 nodes. It first tests that with both 
        /// hosts up, the load is balanced evenly and retries are not used. It then pauses one of the nodes each time and verifies
        /// that the available node is used. Finally, it pauses both nodes and verfifies that a NoHostAvailableException is raised,
        /// and neither host fulfills the query.
        ///
        /// @since 2.7.0
        /// @jira_ticket CSHARP-273
        /// @expected_result For each query, the rety should use the next available host.
        ///
        /// @test_assumptions
        ///    - A Cassandra cluster with 2 nodes
        ///    - A TryNextHostRetryPolicy decision defined, with useCurrentHost set to false
        /// @test_category connection:retry_policy
        [Test]
        public void TryNextHostRetryPolicyTest()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(2000);
            testCluster.Builder = Cluster.Builder()
                                         .WithRetryPolicy(new LoggingRetryPolicy(TryNextHostRetryPolicy.Instance))
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "1")
                                         .AddContactPoint(testCluster.ClusterIpPrefix + "2")
                                         .WithSocketOptions(socketOptions);
            testCluster.InitClient();

            // Setup cluster
            PolicyTestTools policyTestTools = new PolicyTestTools();
            policyTestTools.CreateSchema(testCluster.Session, 2);
            policyTestTools.InitPreparedStatement(testCluster, 12);

            // Try with both hosts
            policyTestTools.Query(testCluster, 10);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + 1 + ":" + DefaultCassandraPort, 5);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + 2 + ":" + DefaultCassandraPort, 5);

            // Try with host 1
            policyTestTools.ResetCoordinators();
            testCluster.PauseNode(2);
            policyTestTools.Query(testCluster, 10);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 1 + ":" + DefaultCassandraPort, 10);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 2 + ":" + DefaultCassandraPort, 0);
            testCluster.ResumeNode(2);

            // Try with host 2
            policyTestTools.ResetCoordinators();
            testCluster.PauseNode(1);
            policyTestTools.Query(testCluster, 10);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 1 + ":" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 2 + ":" + DefaultCassandraPort, 10);

            // Try with 0 hosts
            policyTestTools.ResetCoordinators();
            testCluster.PauseNode(2);
            Assert.Throws<NoHostAvailableException>(() => policyTestTools.Query(testCluster, 10));
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 1 + ":" + DefaultCassandraPort, 0);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + 2 + ":" + DefaultCassandraPort, 0);

            testCluster.ResumeNode(1);
            testCluster.ResumeNode(2);
        }

        private void RetryPolicyTest(ITestCluster testCluster)
        {
            PolicyTestTools policyTestTools = new PolicyTestTools();
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(testCluster.Session, 2);

            // Test before state
            policyTestTools.InitPreparedStatement(testCluster, 12);
            policyTestTools.Query(testCluster, 12);
            int clusterPosQueried = 1;
            int clusterPosNotQueried = 2;
            if (!policyTestTools.Coordinators.ContainsKey(testCluster.ClusterIpPrefix + clusterPosQueried))
            {
                clusterPosQueried = 2;
                clusterPosNotQueried = 1;
            }
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + clusterPosQueried + ":" + DefaultCassandraPort, 1);

            // Stop one of the nodes and test
            policyTestTools.ResetCoordinators();
            testCluster.Stop(clusterPosQueried);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + clusterPosQueried, testCluster.Cluster, 30);
            policyTestTools.Query(testCluster, 120);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + clusterPosNotQueried + ":" + DefaultCassandraPort, 120);
            policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + clusterPosQueried + ":" + DefaultCassandraPort, 0);

            // Start the node that was just down, then down the node that was just up
            policyTestTools.ResetCoordinators();
            testCluster.Start(clusterPosQueried);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + clusterPosQueried, DefaultCassandraPort, 30);

            // Test successful reads
            DateTime futureDateTime = DateTime.Now.AddSeconds(120);
            while (policyTestTools.Coordinators.Count < 2 && DateTime.Now < futureDateTime)
            {
                policyTestTools.Query(testCluster, 120);
                Thread.Sleep(75);
            }

            // Ensure that the nodes were contacted
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + clusterPosQueried + ":" + DefaultCassandraPort, 1);
            policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + clusterPosNotQueried + ":" + DefaultCassandraPort, 1);

        }
    }
}
