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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class ClientWarningsTests : TestGlobals
    {
        public ISession Session { get; set; }

        private const string Keyspace = "ks_client_warnings";
        private const string Table = Keyspace + ".tbl1";

        private ITestCluster _testCluster;

        [OneTimeSetUp]
        public void SetupFixture()
        {
            if (TestClusterManager.CheckCassandraVersion(false, Version.Parse("2.2"), Comparison.LessThan))
            {
                Assert.Ignore("Requires Cassandra version >= 2.2");
                return;
            }

            string[] cassandraYaml = null;
            if (TestClusterManager.CheckCassandraVersion(true, Version.Parse("5.0"), Comparison.GreaterThanOrEqualsTo))
            {
                cassandraYaml = new[]
                {
                    "batch_size_warn_threshold:5KiB",
                    "batch_size_fail_threshold:50KiB"
                };
            }
            else
            {
                cassandraYaml = new[]
                {
                    "batch_size_warn_threshold_in_kb:5",
                    "batch_size_fail_threshold_in_kb:50"
                };
            }

            _testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                CassandraYaml = cassandraYaml,
                //Using a mirroring handler, the server will reply providing the same payload that was sent
                JvmArgs = new[] { "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler" }
            });
            _testCluster.InitClient();
            Session = _testCluster.Session;
            Session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, Keyspace, 1));
            Session.Execute(string.Format(TestUtils.CreateTableSimpleFormat, Table));
        }

        [Test]
        public void Should_QueryTrace_When_Enabled()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local").EnableTracing());
            Assert.NotNull(rs.Info.QueryTrace);
            var hosts = Session.Cluster.AllHosts();
            Assert.NotNull(hosts);
            var coordinator = hosts.FirstOrDefault();
            Assert.NotNull(coordinator);
            Assert.AreEqual(coordinator.Address.Address, rs.Info.QueryTrace.Coordinator);
            Assert.Greater(rs.Info.QueryTrace.Events.Count, 0);
            if (Session.BinaryProtocolVersion >= 4)
            {
                Assert.NotNull(rs.Info.QueryTrace.ClientAddress);   
            }
            else
            {
                Assert.Null(rs.Info.QueryTrace.ClientAddress);
            }
        }

        [Test]
        public void Should_NotGetQueryTrace_When_NotEnabledXDefaultX()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local"));
            Assert.Null(rs.Info.QueryTrace);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_NotGenerateWarning_When_RegularBehavior()
        {
            var rs = Session.Execute("SELECT * FROM system.local");
            //It should be null for queries that do not generate warnings
            Assert.Null(rs.Info.Warnings);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_Warning_When_BatchExceedsLength()
        {
            var rs = Session.Execute(GetBatchAsSimpleStatement(5*1025));
            Assert.NotNull(rs.Info.Warnings);
            Assert.AreEqual(1, rs.Info.Warnings.Length);
            StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
            StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_WarningWithTrace_When_BatchExceedsLengthAndTraceEnabled()
        {
            var statement = GetBatchAsSimpleStatement(5 * 1025);
            var rs = Session.Execute(statement.EnableTracing());
            Assert.NotNull(rs.Info.QueryTrace);
            Assert.NotNull(rs.Info.Warnings);
            Assert.AreEqual(1, rs.Info.Warnings.Length);
            StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
            StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_ThrowInvalidException_When_BatchIsTooBig()
        {
            const int length = 50 * 1025;
            Assert.Throws<InvalidQueryException>(() => Session.Execute(GetBatchAsSimpleStatement(length)));
            Assert.Throws<InvalidQueryException>(() => Session.Execute(
                GetBatchAsSimpleStatement(length).EnableTracing()));
            Assert.Throws<InvalidQueryException>(() => Session.Execute(
                GetBatchAsSimpleStatement(length).EnableTracing().SetOutgoingPayload(GetPayload())));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_WarningAndGetPayload_When_UsingMirrorPayload()
        {
            var statement = GetBatchAsSimpleStatement(5*1025);
            var outgoing = GetPayload();
            var rs = Session.Execute(statement.SetOutgoingPayload(GetPayload()));
            Assert.NotNull(rs.Info.Warnings);
            Assert.AreEqual(1, rs.Info.Warnings.Length);
            StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
            StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
            CollectionAssert.AreEqual(outgoing["k1"], rs.Info.IncomingPayload["k1"]);
            CollectionAssert.AreEqual(outgoing["k2"], rs.Info.IncomingPayload["k2"]);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Should_WarningAndTracingAndGetPayload_When_UsingMirrorPayloadAndEnableTracing()
        {
            var statement = GetBatchAsSimpleStatement(5*1025);
            var outgoing = new Dictionary<string, byte[]>
            {
                {"k1", Encoding.UTF8.GetBytes("value1")},
                {"k2", Encoding.UTF8.GetBytes("value2")}
            };
            var rs = Session.Execute(statement.SetOutgoingPayload(outgoing).EnableTracing());

            Assert.NotNull(rs.Info.QueryTrace);
            CollectionAssert.AreEqual(outgoing["k1"], rs.Info.IncomingPayload["k1"]);
            CollectionAssert.AreEqual(outgoing["k2"], rs.Info.IncomingPayload["k2"]);
            Assert.NotNull(rs.Info.Warnings);
            Assert.AreEqual(1, rs.Info.Warnings.Length);
            StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
            StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
        }

        private static IStatement GetBatchAsSimpleStatement(int length)
        {
            const string query = "BEGIN UNLOGGED BATCH" +
                                 " INSERT INTO {0} (k, t) VALUES ('key0', 'value0');" +
                                 " INSERT INTO {0} (k, t) VALUES ('{1}', '{2}');" +
                                 "APPLY BATCH";
            return new SimpleStatement(string.Format(query, Table, "key1", String.Join("", Enumerable.Repeat("a", length))))
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        }

        private static IDictionary<string, byte[]> GetPayload()
        {
            return new Dictionary<string, byte[]>
            {
                {"k1", Encoding.UTF8.GetBytes("value1")},
                {"k2", Encoding.UTF8.GetBytes("value2")}
            };
        }
    }
}
