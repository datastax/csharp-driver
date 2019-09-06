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

using NUnit.Framework;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ConsistencyTests
    {
        private const string Query = "SELECT id, value from verifyconsistency";

        /// Tests that the default consistency level for queries is LOCAL_ONE
        /// 
        /// LocalOne_Is_Default_Consistency tests that the default consistency level for all queries is LOCAL_ONE. It performs
        /// a simple select statement and verifies that the result set metadata shows that the achieved consistency level is LOCAL_ONE.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-378
        /// @expected_result The default consistency level should be LOCAL_ONE
        /// 
        /// @test_category consistency
        [Test]
        public void Should_UseCLLocalOne_When_NotSpecifiedXDefaultX()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute(new SimpleStatement(Query));
                Assert.AreEqual(ConsistencyLevel.LocalOne, rs.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, "LOCAL_ONE");
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM")]
        [TestCase(ConsistencyLevel.All, "ALL")]
        [TestCase(ConsistencyLevel.Any, "ANY")]
        [TestCase(ConsistencyLevel.One, "ONE")]
        [TestCase(ConsistencyLevel.Two, "TWO")]
        [TestCase(ConsistencyLevel.Three, "THREE")]
        [TestCase(ConsistencyLevel.LocalOne, "LOCAL_ONE")]
        [TestCase(ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM")]
        public void Should_UseQueryOptionsCL_When_NotSetAtSimpleStatement(ConsistencyLevel consistencyLevel, 
                                                                                string consistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(Query);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, consistencyLevelString);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Serial, "SERIAL")]
        [TestCase(ConsistencyLevel.LocalSerial, "LOCAL_SERIAL")]
        public void Should_UseQueryOptionsSerialCL_When_NotSetAtSimpleStatement(ConsistencyLevel serialConsistency, string serialConsistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .WithQueryOptions(new QueryOptions().SetSerialConsistencyLevel(serialConsistency))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                const string conditionalQuery = "insert into tbl_serial (id, value) values (1, 2) if not exists";
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(conditionalQuery);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(ConsistencyLevel.LocalOne, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, conditionalQuery, "LOCAL_ONE", serialConsistencyLevelString);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM")]
        [TestCase(ConsistencyLevel.All, "ALL")]
        [TestCase(ConsistencyLevel.Any, "ANY")]
        [TestCase(ConsistencyLevel.One, "ONE")]
        [TestCase(ConsistencyLevel.Two, "TWO")]
        [TestCase(ConsistencyLevel.Three, "THREE")]
        [TestCase(ConsistencyLevel.LocalOne, "LOCAL_ONE")]
        [TestCase(ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM")]
        public void Should_UseSimpleStatementCL_When_Set(ConsistencyLevel consistencyLevel, string consistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Any))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(Query).SetConsistencyLevel(consistencyLevel);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, consistencyLevelString);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM", ConsistencyLevel.Serial, "SERIAL")]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM", ConsistencyLevel.LocalSerial, "LOCAL_SERIAL")]
        public void Should_UseSerialConsistencyLevelSpecified_When_ConditionalQuery(ConsistencyLevel consistencyLevel, 
                     string consistencyLevelString, ConsistencyLevel serialConsistency, string serialConsistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                const string conditionalQuery = "update tbl_serial set value=2 where id=1 if exists";
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(conditionalQuery)
                                                                .SetConsistencyLevel(consistencyLevel)
                                                                .SetSerialConsistencyLevel(serialConsistency);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, conditionalQuery, consistencyLevelString, serialConsistencyLevelString);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM", ConsistencyLevel.Serial, "SERIAL")]
        [TestCase(ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM", ConsistencyLevel.LocalSerial, "LOCAL_SERIAL")]
        public void Should_UseSerialConsistencyLevel_From_QueryOptions(ConsistencyLevel consistencyLevel,
                                                                       string consistencyLevelString,
                                                                       ConsistencyLevel serialConsistency,
                                                                       string serialConsistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions()
                                            .SetConsistencyLevel(consistencyLevel)
                                            .SetSerialConsistencyLevel(serialConsistency))
                                        .Build())
            {
                const string conditionalQuery = "update tbl_serial set value=3 where id=2 if exists";
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(conditionalQuery);

                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, conditionalQuery, consistencyLevelString,
                                  serialConsistencyLevelString);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM")]
        [TestCase(ConsistencyLevel.All, "ALL")]
        [TestCase(ConsistencyLevel.Any, "ANY")]
        [TestCase(ConsistencyLevel.One, "ONE")]
        [TestCase(ConsistencyLevel.Two, "TWO")]
        [TestCase(ConsistencyLevel.Three, "THREE")]
        [TestCase(ConsistencyLevel.LocalOne, "LOCAL_ONE")]
        [TestCase(ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM")]
        public void Should_UseQueryOptionsCL_When_NotSetAtPreparedStatement(ConsistencyLevel consistencyLevel, 
                                                                                string consistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=?";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, consistencyLevelString, null, "EXECUTE");
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Serial, "SERIAL")]
        [TestCase(ConsistencyLevel.LocalSerial, "LOCAL_SERIAL")]
        public void Should_UseQueryOptionsSerialCL_When_NotSetAtPreparedStatement(ConsistencyLevel consistencyLevel, 
                                                                            string consistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .WithQueryOptions(new QueryOptions().SetSerialConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=? if exists";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(ConsistencyLevel.LocalOne, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, "LOCAL_ONE", consistencyLevelString, "EXECUTE");
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, "QUORUM")]
        [TestCase(ConsistencyLevel.All, "ALL")]
        [TestCase(ConsistencyLevel.Any, "ANY")]
        [TestCase(ConsistencyLevel.One, "ONE")]
        [TestCase(ConsistencyLevel.Two, "TWO")]
        [TestCase(ConsistencyLevel.Three, "THREE")]
        [TestCase(ConsistencyLevel.LocalOne, "LOCAL_ONE")]
        [TestCase(ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM")]
        public void Should_UsePreparedStatementCL_When_Set(ConsistencyLevel consistencyLevel, string consistencyLevelString)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=?";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1).SetConsistencyLevel(consistencyLevel);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, consistencyLevelString, null, "EXECUTE");
            }
        }

        private static void VerifyConsistency(SimulacronCluster simulacronCluster, string query, string consistency, 
                                              string serialConsistency = null, string queryType = "QUERY")
        {
            var executedQueries = simulacronCluster.GetQueries(query, queryType);
            Assert.NotNull(executedQueries);
            var log = executedQueries.First();
            Assert.AreEqual(consistency, log.consistency_level.ToString());
            if (serialConsistency != null)
            {
                Assert.AreEqual(serialConsistency, log.serial_consistency_level.ToString());
            }
        }
    }
}
