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
using System.Linq;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category(TestCategory.Long), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class ConsistencyTests : TestGlobals
    {
        private PolicyTestTools _policyTestTools = null;

        [SetUp]
        public void SetupTest()
        {
            _policyTestTools = new PolicyTestTools();
        }

        /// <summary>
        /// Verify that replication factor one consistency is enforced, 
        /// with load balancing policy TokenAware, RoundRobin
        /// 
        /// @test_category consistency
        /// @test_category connection:outage
        /// @test_category load_balancing:round_robin,token_aware
        /// </summary>
        [Test]
        public void ReplicationFactorOne_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = ClusterBuilder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 1);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.One);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.One);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.Split(':').First();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + awareCoord + ":" + DefaultCassandraPort, 12);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord + ":" + DefaultCassandraPort, testCluster.Cluster, 30);

            var acceptedList = new List<ConsistencyLevel> 
            {
                ConsistencyLevel.Any
            };

            var failList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.One,
                ConsistencyLevel.Two,
                ConsistencyLevel.Three,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.All
            };

            // Test successful writes
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                }
                catch (Exception e)
                {
                    Assert.Fail(string.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
                }
            }

            // Test successful reads
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("It must not pass at consistency level {0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
                catch (WriteTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (ReadTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }
        }

        /// <summary>
        /// Verify that replication factor two is enforced, 
        /// with load balancing policy TokenAware, RoundRobin
        /// 
        /// @test_category consistency
        /// @test_category connection:outage
        /// @test_category load_balancing:round_robin,token_aware
        /// </summary>
        [Test]
        public void ReplicationFactorTwo_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = ClusterBuilder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 2);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.Split(':').First();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            int coordinatorsWithMoreThanZeroQueries = 0;
            foreach (var coordinator in _policyTestTools.Coordinators)
            {
                coordinatorsWithMoreThanZeroQueries++;
                _policyTestTools.AssertQueried(coordinator.Key, 6);
            }
            Assert.AreEqual(2, coordinatorsWithMoreThanZeroQueries);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord, testCluster.Cluster, 30);

            var acceptedList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Any,
                ConsistencyLevel.One
            };

            var failList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Two,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.Three,
                ConsistencyLevel.All
            };

            // Test successful writes
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                }
                catch (Exception e)
                {
                    Assert.Fail(string.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
                }
            }

            // Test successful reads
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
                catch (WriteTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (ReadTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }
        }

        /// <summary>
        /// Verify that replication factor three is enforced, 
        /// with load balancing policy TokenAware, RoundRobin
        /// 
        /// @test_category consistency
        /// @test_category connection:outage
        /// @test_category load_balancing:round_robin,token_aware
        /// </summary>
        [Test]
        public void ReplicationFactorThree_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = ClusterBuilder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            string coordinatorUsedIp = _policyTestTools.Coordinators.First().Key.Split(':').First();
            int awareCoord = int.Parse(coordinatorUsedIp.Split('.').Last());

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1:" + DefaultCassandraPort, 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2:" + DefaultCassandraPort, 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3:" + DefaultCassandraPort, 4);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord, testCluster.Cluster, 30);

            var acceptedList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Any,
                ConsistencyLevel.One,
                ConsistencyLevel.Two,
                ConsistencyLevel.Quorum
            };

            var failList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Three,
                ConsistencyLevel.All
            };

            // Test successful writes
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                }
                catch (Exception e)
                {
                    Assert.Fail(string.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
                }
            }

            // Test successful reads
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
                catch (WriteTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (ReadTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }
        }

        //////////////////////////////
        /// Test Helpers
        //////////////////////////////

        public void TestReplicationFactorThree(ITestCluster testCluster)
        {
            _policyTestTools.CreateSchema(testCluster.Session, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Three);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Three);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(2);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 5);

            var acceptedList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Any,
                ConsistencyLevel.One,
                ConsistencyLevel.Two,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.Three,
                ConsistencyLevel.All
            };

            var failList = new List<ConsistencyLevel>();

            // Test successful writes
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                }
                catch (Exception e)
                {
                    Assert.Fail(string.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
                }
            }

            // Test successful reads
            foreach (ConsistencyLevel consistencyLevel in acceptedList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
                catch (WriteTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.Query(testCluster, 12, consistencyLevel);
                    Assert.Fail(string.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), string.Format("Received: {0}", e.Message));
                }
                catch (ReadTimeoutException)
                {
                    // expected to fail when the client hasn't marked the'
                    // node as DOWN yet
                }
                catch (UnavailableException)
                {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }
        }

    }
}
