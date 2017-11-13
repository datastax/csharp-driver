using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ClientWarningsTests : TestGlobals
    {
        public ISession Session { get; set; }

        private const string Keyspace = "ks_client_warnings";
        private const string Table = Keyspace + ".tbl1";

        [OneTimeSetUp]
        public void SetupFixture()
        {
            if (CassandraVersion < Version.Parse("2.2.0"))
                Assert.Ignore("Requires Cassandra version >= 2.2");

            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            
            var testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                //Using a mirroring handler, the server will reply providing the same payload that was sent
                JvmArgs = new[] { "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler" }
            });
            testCluster.InitClient();
            Session = testCluster.Session;
            Session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, Keyspace, 1));
            Session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, Table));
        }

        [Test]
        public void QueryTraceEnabledTest()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local").EnableTracing());
            Assert.NotNull(rs.Info.QueryTrace);
            Assert.AreEqual(Session.Cluster.AllHosts().FirstOrDefault(), rs.Info.QueryTrace.Coordinator);
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
        public void QueryTraceDisabledByDefaultTest()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local"));
            Assert.Null(rs.Info.QueryTrace);
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
            var rs = Session.Execute(GetBatchAsSimpleStatement(5*1025));
            Assert.NotNull(rs.Info.Warnings);
            Assert.AreEqual(1, rs.Info.Warnings.Length);
            StringAssert.Contains("batch", rs.Info.Warnings[0].ToLowerInvariant());
            StringAssert.Contains("exceeding", rs.Info.Warnings[0].ToLowerInvariant());
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_With_Tracing_Test()
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
        public void Batch_Error_Test()
        {
            const int length = 50 * 1025;
            Assert.Throws<InvalidQueryException>(() => Session.Execute(GetBatchAsSimpleStatement(length)));
            Assert.Throws<InvalidQueryException>(() => Session.Execute(
                GetBatchAsSimpleStatement(length).EnableTracing()));
            Assert.Throws<InvalidQueryException>(() => Session.Execute(
                GetBatchAsSimpleStatement(length).EnableTracing().SetOutgoingPayload(GetPayload())));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Warnings_With_Custom_Payload_Test()
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
        public void Warnings_With_Tracing_And_Custom_Payload_Test()
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

        private static SimpleStatement GetBatchAsSimpleStatement(int length)
        {
            const string query = "BEGIN UNLOGGED BATCH" +
                                 " INSERT INTO {0} (k, t) VALUES ('key0', 'value0');" +
                                 " INSERT INTO {0} (k, t) VALUES ('{1}', '{2}');" +
                                 "APPLY BATCH";
            return new SimpleStatement(
                string.Format(query, Table, "key1", String.Join("", Enumerable.Repeat("a", length))));
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
