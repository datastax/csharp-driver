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

using System.Diagnostics;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies
{
    [TestFixture, Category("long")]
    public class RetryPolicyTests : TestGlobals
    {
        private PolicyTestTools _policyTestTools = null;

        [SetUp]
        public void TestSetup()
        {
            _policyTestTools = new PolicyTestTools();
        }

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
        /// </summary>
        [Test]
        public void LoggingRetryPolicy_DowngradingConsistency()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.Instance));
            DowngradingConsistencyRetryPolicyTest(builder);
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy
        /// </summary>
        public void DowngradingConsistencyRetryPolicyTest(Builder builder)
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = builder;
            testCluster.InitClient();
            _policyTestTools.CreateSchema(testCluster.Session, 3);

            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            TestUtils.WaitForSchemaAgreement(testCluster.Cluster);

            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.All);

            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.All);
            _policyTestTools.AssertAchievedConsistencyLevel(ConsistencyLevel.All);

            //Kill one node: 2 nodes alive
            testCluster.Stop(2);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 20);

            //After killing one node, the achieved consistency level should be downgraded
            _policyTestTools.ResetCoordinators();
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.All);
            _policyTestTools.AssertAchievedConsistencyLevel(ConsistencyLevel.Two);
        }

        /// <summary>
        /// Test AlwaysIgnoreRetryPolicy with Logging enabled
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
            _policyTestTools.CreateSchema(testCluster.Session, 2);

            // Test before state
            _policyTestTools.InitPreparedStatement(testCluster, 12);
            _policyTestTools.Query(testCluster, 12);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 6);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 6);

            // Stop one of the nodes and test
            _policyTestTools.ResetCoordinators();
            testCluster.Stop(2);
            string hostRecentlyStopped = testCluster.ClusterIpPrefix + "2";
            TestUtils.WaitForDown(hostRecentlyStopped, testCluster.Cluster, 30);
            _policyTestTools.Query(testCluster, 120);

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 120);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);

            // Start node and test
            _policyTestTools.ResetCoordinators();
            testCluster.Start(2);
            TestUtils.WaitForUp(hostRecentlyStopped, testCluster.Builder, 30);

            // Test successful reads
            _policyTestTools.Query(testCluster, 120);

            // Ensure that the nodes were contacted
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "1", 1);
            _policyTestTools.AssertQueriedAtLeast(testCluster.ClusterIpPrefix + "2", 1);
            _policyTestTools.ResetCoordinators();

        }



    }
}
