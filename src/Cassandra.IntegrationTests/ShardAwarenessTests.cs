using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using Cassandra.Connections.Control;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class ShardAwarenessTest : TestGlobals
    {
        private ITestCluster _realCluster;

        [TearDown]
        public void TestTearDown()
        {
            TestClusterManager.TryRemove();
            _realCluster = null;
        }

        [Test]
        public void CorrectShardInTracingTest()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var _session = cluster.Connect();

            _session.Execute("DROP KEYSPACE IF EXISTS shardawaretest");
            _session.Execute("CREATE KEYSPACE shardawaretest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}");
            _session.Execute("CREATE TABLE shardawaretest.t (pk text, ck text, v text, PRIMARY KEY (pk, ck))");

            var populateStatement = _session.Prepare("INSERT INTO shardawaretest.t (pk, ck, v) VALUES (?, ?, ?)");
            _session.Execute(populateStatement.Bind("a", "b", "c"));
            _session.Execute(populateStatement.Bind("e", "f", "g"));
            _session.Execute(populateStatement.Bind("100002", "f", "g"));

            VerifyCorrectShardSingleRow(_session, "a", "b", "c", "shard 0");
            VerifyCorrectShardSingleRow(_session, "e", "f", "g", "shard 0");
            VerifyCorrectShardSingleRow(_session, "100002", "f", "g", "shard 1");
        }

        private void VerifyCorrectShardSingleRow(ISession _session, string pk, string ck, string v, string shard)
        {
            var prepared = _session.Prepare("SELECT pk, ck, v FROM shardawaretest.t WHERE pk=? AND ck=?");
            var result = _session.Execute(prepared.Bind(pk, ck).EnableTracing());

            var row = result.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(pk, row.GetValue<string>("pk"));
            Assert.AreEqual(ck, row.GetValue<string>("ck"));
            Assert.AreEqual(v, row.GetValue<string>("v"));

            var executionInfo = result.Info;
            var trace = executionInfo.QueryTrace;
            bool anyLocal = false;
            foreach (var eventItem in trace.Events)
            {
                Trace.TraceInformation("  {0} - {1} - [{2}] - {3}",
                    eventItem.SourceElapsedMicros,
                    eventItem.Source,
                    eventItem.ThreadName,
                    eventItem.Description);
                Assert.IsTrue(eventItem.ThreadName.StartsWith(shard));
                if (eventItem.Description.Contains("querying locally"))
                {
                    anyLocal = true;
                }
            }
            Assert.IsTrue(anyLocal);
        }
    }
}
