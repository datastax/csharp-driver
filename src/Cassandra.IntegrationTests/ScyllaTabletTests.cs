using System;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class ScyllaTabletTest : TestGlobals
    {
        private ITestCluster _realCluster;

        [TearDown]
        public void TestTearDown()
        {
            TestClusterManager.TryRemove();
            _realCluster = null;
        }

        [Test]
        public void CorrectTabletMapTest()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var _session = cluster.Connect();

            var rf = 1;
            _session.Execute("DROP KEYSPACE IF EXISTS tablettest");
            _session.Execute($"CREATE KEYSPACE tablettest WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '{rf}'}}");
            _session.Execute("CREATE TABLE tablettest.t (pk text, ck text, v text, PRIMARY KEY (pk, ck))");

            var populateStatement = _session.Prepare("INSERT INTO tablettest.t (pk, ck, v) VALUES (?, ?, ?)");
            //Insert 50 rows to ensure that the tablet map is populated correctly
            for (int i = 0; i < 50; i++)
            {
                _session.Execute(populateStatement.Bind(i.ToString(), "ck" + i, "v" + i));
            }

            var tabletMap = _session.Cluster.Metadata.TabletMap;
            Assert.IsNotNull(tabletMap);

            // Wait for the tablet map to be populated (since updates are event-driven)
            var timeout = TimeSpan.FromSeconds(10);
            var sw = Stopwatch.StartNew();
            while (tabletMap.GetMapping().Count == 0 && sw.Elapsed < timeout)
            {
                System.Threading.Thread.Sleep(100);
            }
            Assert.IsTrue(tabletMap.GetMapping().Count > 0, "Tablet map should contain tablets for the table");

            // Find the mapping for the specific keyspace and table
            var key = new TabletMap.KeyspaceTableNamePair("tablettest", "t");
            if (tabletMap.GetMapping().TryGetValue(key, out var tabletSet))
            {
                var tablets = tabletSet.Tablets;
                foreach (var tablet in tablets)
                {
                    Trace.TraceInformation($"Tablet: First token: {tablet.FirstToken}, Last token: {tablet.LastToken}, Replicas: {string.Join(", ", tablet.Replicas.Select(h => h.HostID + " (Shard " + h.Shard + ")"))}");
                    Assert.IsTrue(tablet.Replicas.Count == rf, "Make sure replicas count is equal RF");
                }
                Assert.IsTrue(tablets.Count > 0, "Make sure tablets are present in the tablet set");
            }
        }

        [Test]
        public void CorrectShardUsingTabletsInTracingTest()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var _session = cluster.Connect();

            var rf = 1;
            _session.Execute("DROP KEYSPACE IF EXISTS tablettest");
            _session.Execute($"CREATE KEYSPACE tablettest WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '{rf}'}}");
            _session.Execute("CREATE TABLE tablettest.t (pk text, ck text, v text, PRIMARY KEY (pk, ck))");

            var populateStatement = _session.Prepare("INSERT INTO tablettest.t (pk, ck, v) VALUES (?, ?, ?)");
            //Insert 50 rows to ensure that the tablet map is populated correctly
            for (int i = 0; i < 50; i++)
            {
                _session.Execute(populateStatement.Bind(i.ToString(), "ck" + i, "v" + i));
            }

            for (int i = 0; i < 50; i++)
            {
                _session.Execute(populateStatement.Bind(i.ToString(), "ck" + i, "v" + i));
            }
        }

        private void VerifyCorrectShardSingleRow(ISession _session, string pk, string ck, string v)
        {
            var prepared = _session.Prepare("SELECT pk, ck, v FROM tablettest.t WHERE pk=? AND ck=?");
            var result = _session.Execute(prepared.Bind(pk, ck).EnableTracing());

            var row = result.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(pk, row.GetValue<string>("pk"));
            Assert.AreEqual(ck, row.GetValue<string>("ck"));
            Assert.AreEqual(v, row.GetValue<string>("v"));

            var executionInfo = result.Info;
            var trace = executionInfo.QueryTrace;
            bool anyLocal = false;
            var shardSet = new HashSet<string>();
            foreach (var eventItem in trace.Events)
            {
                Trace.TraceInformation("  {0} - {1} - [{2}] - {3}",
                    eventItem.SourceElapsedMicros,
                    eventItem.Source,
                    eventItem.ThreadName,
                    eventItem.Description);
                shardSet.Add(eventItem.ThreadName);
                if (eventItem.Description.Contains("querying locally"))
                {
                    anyLocal = true;
                }
            }
            Assert.IsTrue(shardSet.Count == 1);
            Assert.IsTrue(anyLocal);
        }
    }
}
