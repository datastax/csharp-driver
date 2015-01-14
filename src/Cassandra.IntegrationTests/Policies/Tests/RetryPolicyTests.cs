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
using System.Threading;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("long")]
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
