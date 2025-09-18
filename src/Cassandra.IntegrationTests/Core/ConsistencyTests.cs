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
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using System.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class ConsistencyTests : TestGlobals
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
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute(new SimpleStatement(Query));
                Assert.AreEqual(ConsistencyLevel.LocalOne, rs.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, ConsistencyLevel.LocalOne);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum)]
        [TestCase(ConsistencyLevel.All)]
        [TestCase(ConsistencyLevel.Any)]
        [TestCase(ConsistencyLevel.One)]
        [TestCase(ConsistencyLevel.Two)]
        [TestCase(ConsistencyLevel.Three)]
        [TestCase(ConsistencyLevel.LocalOne)]
        [TestCase(ConsistencyLevel.LocalQuorum)]
        public void Should_UseQueryOptionsCL_When_NotSetAtSimpleStatement(ConsistencyLevel consistencyLevel)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(Query);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, consistencyLevel);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Serial)]
        [TestCase(ConsistencyLevel.LocalSerial)]
        public void Should_UseQueryOptionsSerialCL_When_NotSetAtSimpleStatement(ConsistencyLevel serialConsistency)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .WithQueryOptions(new QueryOptions().SetSerialConsistencyLevel(serialConsistency))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                const string conditionalQuery = "insert into tbl_serial (id, value) values (1, 2) if not exists";
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(conditionalQuery);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(ConsistencyLevel.LocalOne, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, conditionalQuery, ConsistencyLevel.LocalOne, serialConsistency);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum)]
        [TestCase(ConsistencyLevel.All)]
        [TestCase(ConsistencyLevel.Any)]
        [TestCase(ConsistencyLevel.One)]
        [TestCase(ConsistencyLevel.Two)]
        [TestCase(ConsistencyLevel.Three)]
        [TestCase(ConsistencyLevel.LocalOne)]
        [TestCase(ConsistencyLevel.LocalQuorum)]
        public void Should_UseSimpleStatementCL_When_Set(ConsistencyLevel consistencyLevel)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Any))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(Query).SetConsistencyLevel(consistencyLevel);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, Query, consistencyLevel);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, ConsistencyLevel.Serial)]
        [TestCase(ConsistencyLevel.Quorum, ConsistencyLevel.LocalSerial)]
        public void Should_UseSerialConsistencyLevelSpecified_When_ConditionalQuery(
            ConsistencyLevel consistencyLevel, ConsistencyLevel serialConsistency)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                const string conditionalQuery = "update tbl_serial set value=2 where id=1 if exists";
                var session = cluster.Connect();
                var simpleStatement = new SimpleStatement(conditionalQuery)
                                                                .SetConsistencyLevel(consistencyLevel)
                                                                .SetSerialConsistencyLevel(serialConsistency);
                var result = session.Execute(simpleStatement);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, conditionalQuery, consistencyLevel, serialConsistency);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum, ConsistencyLevel.Serial)]
        [TestCase(ConsistencyLevel.LocalQuorum, ConsistencyLevel.LocalSerial)]
        public void Should_UseSerialConsistencyLevel_From_QueryOptions(
            ConsistencyLevel consistencyLevel, ConsistencyLevel serialConsistency)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
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
                VerifyConsistency(simulacronCluster, conditionalQuery, consistencyLevel, serialConsistency);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum)]
        [TestCase(ConsistencyLevel.All)]
        [TestCase(ConsistencyLevel.Any)]
        [TestCase(ConsistencyLevel.One)]
        [TestCase(ConsistencyLevel.Two)]
        [TestCase(ConsistencyLevel.Three)]
        [TestCase(ConsistencyLevel.LocalOne)]
        [TestCase(ConsistencyLevel.LocalQuorum)]
        public void Should_UseQueryOptionsCL_When_NotSetAtPreparedStatement(ConsistencyLevel consistencyLevel)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=?";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, consistencyLevel, null, QueryType.Execute);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Serial)]
        [TestCase(ConsistencyLevel.LocalSerial)]
        public void Should_UseQueryOptionsSerialCL_When_NotSetAtPreparedStatement(ConsistencyLevel consistencyLevel)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .WithQueryOptions(new QueryOptions().SetSerialConsistencyLevel(consistencyLevel))
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=? if exists";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(ConsistencyLevel.LocalOne, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, ConsistencyLevel.LocalOne, consistencyLevel, QueryType.Execute);
            }
        }

        [Test]
        [TestCase(ConsistencyLevel.Quorum)]
        [TestCase(ConsistencyLevel.All)]
        [TestCase(ConsistencyLevel.Any)]
        [TestCase(ConsistencyLevel.One)]
        [TestCase(ConsistencyLevel.Two)]
        [TestCase(ConsistencyLevel.Three)]
        [TestCase(ConsistencyLevel.LocalOne)]
        [TestCase(ConsistencyLevel.LocalQuorum)]
        public void Should_UsePreparedStatementCL_When_Set(ConsistencyLevel consistencyLevel)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3,3" }))
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                const string prepQuery = "select id, value from tbl_consistency where id=?";
                var prepStmt = session.Prepare(prepQuery);
                var boundStmt = prepStmt.Bind(1).SetConsistencyLevel(consistencyLevel);
                var result = session.Execute(boundStmt);
                Assert.AreEqual(consistencyLevel, result.Info.AchievedConsistency);
                VerifyConsistency(simulacronCluster, prepQuery, consistencyLevel, null, QueryType.Execute);
            }
        }

        private static void VerifyConsistency(SimulacronCluster simulacronCluster, string query, ConsistencyLevel consistency,
                                              ConsistencyLevel? serialConsistency = null, QueryType queryType = QueryType.Query)
        {
            var executedQueries = simulacronCluster.GetQueries(query, queryType);
            Assert.NotNull(executedQueries);
            var log = executedQueries.First();
            Assert.AreEqual(consistency, log.ConsistencyLevel);

            if (serialConsistency == null)
            {
                Assert.AreEqual(ConsistencyLevel.Serial, log.SerialConsistencyLevel);
            }
            else
            {
                Assert.AreEqual(serialConsistency, log.SerialConsistencyLevel);
            }
        }
    }
}
