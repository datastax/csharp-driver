//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category("short")]
    public class ClientWarningsTests : TestGlobals
    {
        public ISession Session { get; set; }

        private const string Keyspace = "ks_client_warnings";
        private const string Table = Keyspace + ".tbl1";

        private ITestCluster _testCluster;

        [OneTimeSetUp]
        public void SetupFixture()
        {
            if (CassandraVersion < Version.Parse("2.2"))
                Assert.Ignore("Requires Cassandra version >= 2.2");

            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            //Using a mirroring handler, the server will reply providing the same payload that was sent
            var jvmArgs = new[] {"-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"};
            _testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions()
            {
                CassandraYaml = new[] {"batch_size_warn_threshold_in_kb:5" },
                JvmArgs = jvmArgs
            });
            _testCluster.InitClient();
            Session = _testCluster.Session;
            Session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, Keyspace, 1));
            Session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, Table));
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _testCluster.Remove();
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_Is_Null_Test()
        {
            var rs = Session.Execute("SELECT * FROM system.local");
            //It should be null for queries that do not generate warnings
            Assert.Null(rs.Info.Warnings);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_Batch_Exceeding_Length_Test()
        {
            const string query = "BEGIN UNLOGGED BATCH INSERT INTO {0} (k, t) VALUES ('{1}', '{2}') APPLY BATCH";
            var rs = Session.Execute(String.Format(query, Table, "warn1", String.Join("", Enumerable.Repeat("a", 5*1025))));

            //at cassandra 3.6 warnings changed according to CASSANDRA-10876
            var warningsChangingVersion = new Version(3, 6);
            if (CassandraVersion < warningsChangingVersion)
            {
                Assert.NotNull(rs.Info.Warnings);
                Assert.AreEqual(1, rs.Info.Warnings.Length);
                StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
                StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
            }
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_With_Tracing_Test()
        {
            const string query = "BEGIN UNLOGGED BATCH INSERT INTO {0} (k, t) VALUES ('{1}', '{2}') APPLY BATCH";
            SimpleStatement insert = new SimpleStatement(String.Format(query, Table, "warn1", String.Join("", Enumerable.Repeat("a", 5*1025))));
            var rs = Session.Execute(insert.EnableTracing());

            Assert.NotNull(rs.Info.QueryTrace);
            //at cassandra 3.6 warnings changed according to CASSANDRA-10876
            var warningsChangingVersion = new Version(3, 6);
            if (CassandraVersion.CompareTo(warningsChangingVersion) < 0)
            {
                Assert.NotNull(rs.Info.Warnings);
                Assert.AreEqual(1, rs.Info.Warnings.Length);
                StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
                StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
            }
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_With_Custom_Payload_Test()
        {
            const string query = "BEGIN UNLOGGED BATCH INSERT INTO {0} (k, t) VALUES ('{1}', '{2}') APPLY BATCH";
            SimpleStatement insert = new SimpleStatement(String.Format(query, Table, "warn1", String.Join("", Enumerable.Repeat("a", 5*1025))));
            var outgoing = new Dictionary<string, byte[]> {{"k1", Encoding.UTF8.GetBytes("value1")}, {"k2", Encoding.UTF8.GetBytes("value2")}};
            insert.SetOutgoingPayload(outgoing);
            var rs = Session.Execute(insert);

            //at cassandra 3.6 warnings changed according to CASSANDRA-10876
            var warningsChangingVersion = new Version(3, 6);
            if (CassandraVersion.CompareTo(warningsChangingVersion) < 0)
            {
                Assert.NotNull(rs.Info.Warnings);
                Assert.AreEqual(1, rs.Info.Warnings.Length);
                StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
                StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
                CollectionAssert.AreEqual(outgoing["k1"], rs.Info.IncomingPayload["k1"]);
                CollectionAssert.AreEqual(outgoing["k2"], rs.Info.IncomingPayload["k2"]);
            }
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_With_Tracing_And_Custom_Payload_Test()
        {
            const string query = "BEGIN UNLOGGED BATCH INSERT INTO {0} (k, t) VALUES ('{1}', '{2}') APPLY BATCH";
            SimpleStatement insert = new SimpleStatement(String.Format(query, Table, "warn1", String.Join("", Enumerable.Repeat("a", 5*1025))));
            var outgoing = new Dictionary<string, byte[]> {{"k1", Encoding.UTF8.GetBytes("value1")}, {"k2", Encoding.UTF8.GetBytes("value2")}};
            insert.SetOutgoingPayload(outgoing);
            var rs = Session.Execute(insert.EnableTracing());

            Assert.NotNull(rs.Info.QueryTrace);
            CollectionAssert.AreEqual(outgoing["k1"], rs.Info.IncomingPayload["k1"]);
            CollectionAssert.AreEqual(outgoing["k2"], rs.Info.IncomingPayload["k2"]);
            //at cassandra 3.6 warnings changed according to CASSANDRA-10876
            var warningsChangingVersion = new Version(3, 6);
            if (CassandraVersion.CompareTo(warningsChangingVersion) < 0)
            {
                Assert.NotNull(rs.Info.Warnings);
                Assert.AreEqual(1, rs.Info.Warnings.Length);
                StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
                StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
            }
        }
    }
}
