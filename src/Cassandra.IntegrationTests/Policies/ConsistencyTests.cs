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
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies
{
    [TestFixture, Category("long")]
    public class ConsistencyTests : TestGlobals
    {
        private PolicyTestTools _policyTestTools = null;

        [SetUp]
        public void TestSetup()
        {
            _policyTestTools = new PolicyTestTools();
        }

        [Test]
        public void ReplicationFactorOne_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 1);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.One);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.One);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.ToString();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + awareCoord, 12);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord, testCluster.Cluster, 30);

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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("It must not pass at consistency level {0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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

        [Test]
        public void ReplicationFactorTwo_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 2);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.ToString();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            int coordinatorsWithMoreThanZeroQueries = 0;
            foreach (var coordinator in _policyTestTools.Coordinators)
            {
                coordinatorsWithMoreThanZeroQueries++;
                _policyTestTools.AssertQueried(coordinator.Key.ToString(), 6);
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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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

        [Test]
        public void ReplicationFactorThree_TokenAware()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            string coordinatorUsedIp = _policyTestTools.Coordinators.First().Key.ToString();
            int awareCoord = int.Parse(coordinatorUsedIp.Split('.').Last());

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 4);

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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
        /// 
        /// </summary>
        [Test]
        public void ReplicationFactorOne_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                                         .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();
            _policyTestTools.CreateSchema(testCluster.Session, 1);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.One);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.One);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.ToString();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + awareCoord, 12);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord, testCluster.Cluster, 30);

            var acceptedList = new List<ConsistencyLevel> {ConsistencyLevel.Any};

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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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

        [Test]
        public void ReplicationFactorTwo_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                                         .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();

            _policyTestTools.CreateSchema(testCluster.Session, 2);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            string coordinatorHostQueried = _policyTestTools.Coordinators.First().Key.ToString();
            int awareCoord = int.Parse(coordinatorHostQueried.Split('.').Last());

            int coordinatorsWithMoreThanZeroQueries = 0;
            foreach (var coordinator in _policyTestTools.Coordinators)
            {
                coordinatorsWithMoreThanZeroQueries++;
                _policyTestTools.AssertQueried(coordinator.Key.ToString(), 6);
            }
            Assert.AreEqual(2, coordinatorsWithMoreThanZeroQueries);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(awareCoord);
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + awareCoord, testCluster.Cluster, 30);

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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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

        [Test]
        public void ReplicationFactorThree_TwoDCs_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3, 3, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                                         .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();

            _policyTestTools.CreateMultiDcSchema(testCluster.Session, 3, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);

            // a maximum of 4 IPs should have returned values for the query -- two copies per each of the two DCs
            int queriesPerIteration = 4;
            int queriesCompleted = 0;
            int actualTries = 0;
            int maxTries = 20;
            while (_policyTestTools.Coordinators.Count() < 4 && actualTries < maxTries)
            {
                _policyTestTools.Query(testCluster, queriesPerIteration, ConsistencyLevel.Two);
                queriesCompleted += queriesPerIteration;
            }

            Assert.AreEqual(4, _policyTestTools.Coordinators.Count(), "The minimum number of hosts queried was not met!");
            int totalQueriesForAllHosts = _policyTestTools.Coordinators.Sum(c => c.Value);
            Assert.AreEqual(queriesCompleted, totalQueriesForAllHosts, 
                "The sum of queries for all hosts should equal the number of queries recorded by the calling test!");

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(2);
            // FIXME: This sleep is needed to allow the waitFor() to work
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 5);

            var acceptedList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Any,
                ConsistencyLevel.One,
                ConsistencyLevel.Two,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.Three,
                ConsistencyLevel.All,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.EachQuorum
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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail("Expected Exception was not thrown for ConsistencyLevel :" + consistencyLevel);
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
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

        [Test]
        public void ReplicationFactorThree_TwoDcs_DcAware_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3, 3, DefaultMaxClusterCmdRetries, true);
            testCluster.Builder = Cluster.Builder()
                                         .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc2")))
                                         .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();

            _policyTestTools.CreateMultiDcSchema(testCluster.Session, 3, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.Two);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.Two);

            // Validate expected number of host / query counts
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "1", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "2", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "3", 0);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "4", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "5", 4);
            _policyTestTools.AssertQueried(testCluster.ClusterIpPrefix + "6", 4);

            _policyTestTools.ResetCoordinators();
            testCluster.StopForce(2);
            // FIXME: This sleep is needed to allow the waitFor() to work
            TestUtils.WaitForDownWithWait(testCluster.ClusterIpPrefix + "2", testCluster.Cluster, 5);

            var acceptedList = new List<ConsistencyLevel>
            {
                ConsistencyLevel.Any,
                ConsistencyLevel.One,
                ConsistencyLevel.Two,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.Three,
                ConsistencyLevel.All,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.EachQuorum
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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                }
            }

            // Test writes which should fail
            foreach (ConsistencyLevel consistencyLevel in failList)
            {
                try
                {
                    _policyTestTools.InitPreparedStatement(testCluster, 12, consistencyLevel);
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
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

        [Test]
        public void ReplicationFactorThree_RoundRobin_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy()).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();
            TestReplicationFactorThree(testCluster);
        }

        [Test]
        public void ReplicationFactorThree_TokenAware_DowngradingConsistencyRetryPolicy()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(3);
            testCluster.Builder = Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testCluster.InitClient();
            TestReplicationFactorThree(testCluster);
        }

        //////////////////////////////
        /// Test Helpers
        //////////////////////////////

        public void TestReplicationFactorThree(ITestCluster testCluster)
        {
            _policyTestTools.CreateSchema(testCluster.Session, 3);
            _policyTestTools.InitPreparedStatement(testCluster, 12, ConsistencyLevel.All);
            _policyTestTools.Query(testCluster, 12, ConsistencyLevel.All);

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
                    Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", consistencyLevel, e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
                    Assert.Fail(String.Format("Test passed at CL.{0}.", consistencyLevel));
                }
                catch (InvalidQueryException e)
                {
                    var acceptableErrorMessages = new List<string>
                    {
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                    };
                    Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
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
