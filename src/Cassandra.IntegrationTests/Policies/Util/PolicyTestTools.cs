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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Util
{
    public class PolicyTestTools
    {
        public string TableName;

        // Coordinators are: Dictionary<(string>HostIpThatWasQueriedAtLeastOnce, (int)NumberOfTimesThisHostWasQueried>
        public Dictionary<string, int> Coordinators = new Dictionary<string, int>();
        public List<ConsistencyLevel> AchievedConsistencies = new List<ConsistencyLevel>();
        public PreparedStatement PreparedStatement;
        public string DefaultKeyspace = "policytesttoolsks";

        public PolicyTestTools()
        {
            TableName = TestUtils.GetUniqueTableName();
        }

        /// <summary>
        ///  Create schemas for the policy tests, depending on replication factors/strategies.
        /// </summary>
        public void CreateSchema(ISession session)
        {
            CreateSchema(session, 1);
        }

        public void CreateSchema(ISession session, int replicationFactor)
        {
            try
            {
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, DefaultKeyspace, replicationFactor));
            }
            catch (AlreadyExistsException)
            {
            }
            TestUtils.WaitForSchemaAgreement(session.Cluster);
            session.ChangeKeyspace(DefaultKeyspace);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TableName));
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }

        public void CreateMultiDcSchema(ISession session, int dc1RF = 1, int dc2RF = 1)
        {
            session.Execute(String.Format(TestUtils.CreateKeyspaceGenericFormat, DefaultKeyspace, "NetworkTopologyStrategy",
                                              string.Format("'dc1' : {0}, 'dc2' : {1}", dc1RF, dc2RF)));
            TestUtils.WaitForSchemaAgreement(session.Cluster);
            session.ChangeKeyspace(DefaultKeyspace);
            TestUtils.WaitForSchemaAgreement(session.Cluster);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TableName));
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }

        ///  Coordinator management/count
        public void AddCoordinator(string coordinator, ConsistencyLevel consistencyLevel)
        {
            if (!Coordinators.ContainsKey(coordinator))
                Coordinators.Add(coordinator, 0);
            int n = Coordinators[coordinator];
            Coordinators[coordinator] = n + 1;
            AchievedConsistencies.Add(consistencyLevel);
        }

        public void ResetCoordinators()
        {
            Coordinators.Clear();
            AchievedConsistencies.Clear();
        }

        public void AssertQueried(string host, int expectedHostQueryCount)
        {
            // if the host is not found, that's the same as it being queried zero times
            int actualHostQueryCount = 0; 
            if (Coordinators.ContainsKey(host))
                actualHostQueryCount = Coordinators[host];
            Assert.AreEqual(expectedHostQueryCount, actualHostQueryCount, "For " + host);
        }

        /// <summary>
        ///     Verifies that exactly numberOfInsertsToMake queries were received by the specified set of hosts.  Request distribution within 
        ///     the set is not tested.
        /// </summary>
        public void AssertQueriedSet(String[] hosts, int n)
        {
            int queriedInSet = 0;
            foreach (var host in hosts)
            {
                queriedInSet += Coordinators.ContainsKey(host) ? (int) Coordinators[host] : 0;
            }
            Assert.AreEqual(queriedInSet, n, String.Format("For [{0}]", String.Join(", ", hosts)));
        }

        public void AssertQueriedAtLeast(string host, int n)
        {
            var queried = 0;
            if (Coordinators.ContainsKey(host))
            {
                queried = Coordinators[host];   
            }
            Assert.GreaterOrEqual(queried, n);
        }

        /// <summary>
        /// Asserts that all consistencies achieved in the last execution are equal to the consistency passed 
        /// </summary>
        public void AssertAchievedConsistencyLevel(ConsistencyLevel expectedConsistency)
        {
            Assert.True(AchievedConsistencies.All(consistency => consistency == expectedConsistency), "Not all consistencies achieved are " + expectedConsistency);
        }

        /// <summary>
        ///  Init methods that handle writes using batch and consistency options.
        /// </summary>
        public void InitPreparedStatement(ITestCluster testCluster, int numberOfInsertsToMake)
        {
            InitPreparedStatement(testCluster, numberOfInsertsToMake, false, ConsistencyLevel.One);
        }

        public void InitPreparedStatement(ITestCluster testCluster, int numberOfInsertsToMake, bool batch)
        {
            InitPreparedStatement(testCluster, numberOfInsertsToMake, batch, ConsistencyLevel.One);
        }

        public void InitPreparedStatement(ITestCluster testCluster, int numberOfInsertsToMake, ConsistencyLevel consistencyLevel)
        {
            InitPreparedStatement(testCluster, numberOfInsertsToMake, false, consistencyLevel);
        }

        public void InitPreparedStatement(ITestCluster testCluster, int numberOfInsertsToMake, bool batch, ConsistencyLevel consistencyLevel)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test
            for (int i = 0; i < numberOfInsertsToMake; ++i)
                if (batch)
                    // BUG: WriteType == SIMPLE                    
                {
                    var bth = new StringBuilder();
                    bth.AppendLine("BEGIN BATCH");
                    bth.AppendLine(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TableName));
                    bth.AppendLine("APPLY BATCH");

                    testCluster.Session.Execute(new SimpleStatement(bth.ToString()).SetConsistencyLevel(consistencyLevel));
                }
                else
                {
                    testCluster.Session.Execute(
                            new SimpleStatement(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TableName)).SetConsistencyLevel(consistencyLevel));
                }

            PreparedStatement = testCluster.Session.Prepare("SELECT * FROM " + TableName + " WHERE k = ?").SetConsistencyLevel(consistencyLevel);
        }


        /// <summary>
        ///  Query methods that handle reads based on PreparedStatements and/or
        ///  ConsistencyLevels.
        /// </summary>
        public void Query(ITestCluster testCluster, int numberOfQueriesToExecute)
        {
            Query(testCluster, numberOfQueriesToExecute, false, ConsistencyLevel.One);
        }

        public void Query(ITestCluster testCluster, int numberOfQueriesToExecute, bool usePrepared)
        {
            Query(testCluster, numberOfQueriesToExecute, usePrepared, ConsistencyLevel.One);
        }

        public void Query(ITestCluster testCluster, int numberOfQueriesToExecute, ConsistencyLevel consistencyLevel)
        {
            Query(testCluster, numberOfQueriesToExecute, false, consistencyLevel);
        }

        public void Query(ITestCluster testCluster, int numberOfQueriesToExecute, bool usePrepared, ConsistencyLevel consistencyLevel)
        {
            if (usePrepared)
            {
                BoundStatement bs = PreparedStatement.Bind(0);
                for (int i = 0; i < numberOfQueriesToExecute; ++i)
                {
                    ConsistencyLevel cac;
                    var rs = testCluster.Session.Execute(bs);
                    {
                        string queriedHost = rs.Info.QueriedHost.ToString();
                        cac = rs.Info.AchievedConsistency;
                        AddCoordinator(queriedHost, cac);
                    }
                }
            }
            else
            {
                var routingKey = new RoutingKey();
                routingKey.RawRoutingKey = Enumerable.Repeat((byte) 0x00, 4).ToArray();
                for (int i = 0; i < numberOfQueriesToExecute; ++i)
                {
                    string hostQueried;
                    ConsistencyLevel achievedConsistency;
                    var rs = testCluster.Session.Execute(
                                new SimpleStatement(String.Format("SELECT * FROM {0} WHERE k = 0", TableName)).SetRoutingKey(routingKey)
                                                                                                          .SetConsistencyLevel(consistencyLevel));
                    {
                        hostQueried = rs.Info.QueriedHost.ToString();
                        achievedConsistency = rs.Info.AchievedConsistency;
                        Trace.TraceInformation("Query {0} executed by {1} with consistency {2}", i, hostQueried, achievedConsistency);
                    }
                    AddCoordinator(hostQueried, achievedConsistency);
                }
            }
        }

        public void WaitForPolicyToolsQueryToHitBootstrappedIp(ITestCluster testCluster, string newlyBootstrappedIp)
        {
            int secondsToPoll = 120;
            DateTime futureDateTime = DateTime.Now.AddSeconds(120);
            Trace.TraceInformation("Polling for " + secondsToPoll + " seconds while we wait for bootstrapped IP to join the ring, be found by the client");
            while (!Coordinators.ContainsKey(newlyBootstrappedIp) && DateTime.Now < futureDateTime)
            {
                try
                {
                    Query(testCluster, 10);
                }
                catch (Exception e)
                {
                    string[] expectedErrMessages =
                    {
                        "Keyspace '" + DefaultKeyspace + "' does not exist",
                        "unconfigured columnfamily",
                        "Cassandra timeout during read query at consistency"
                    };
                    Assert.IsTrue(e.Message.Contains(expectedErrMessages[0]) || e.Message.Contains(expectedErrMessages[1]) || e.Message.Contains(expectedErrMessages[2]),
                        "Error message '" + e.Message + "' did not contain one of these acceptable error messages: " + string.Join(",", expectedErrMessages));
                    Trace.TraceInformation("Caught acceptable one of these acceptable error messages: " + string.Join(",", expectedErrMessages));
                }
                Thread.Sleep(250);
            }
        }

    }
}
