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

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class MetadataTests : TestGlobals
    {
        private const int DefaultNodeCount = 1;

        [OneTimeSetUp]
        public void FixureSetup()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        /// <summary>
        /// When there is a change in schema, it should be received via ControlConnection
        /// This also checks validates keyspace case sensitivity
        /// </summary>
        [Test]
        public void KeyspacesMetadataUpToDateViaCassandraEvents()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            var initialLength = cluster.Metadata.GetKeyspaces().Count;
            Assert.Greater(initialLength, 0);

            //GetReplicas should yield the primary replica when the Keyspace is not found
            Assert.AreEqual(1, cluster.GetReplicas("ks2", new byte[] {0, 0, 0, 1}).Count);

            const string createKeyspaceQuery = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";
            session.Execute(String.Format(createKeyspaceQuery, "ks1", "SimpleStrategy", "'replication_factor' : 1"));
            session.Execute(String.Format(createKeyspaceQuery, "ks2", "SimpleStrategy", "'replication_factor' : 3"));
            session.Execute(String.Format(createKeyspaceQuery, "ks3", "NetworkTopologyStrategy", "'dc1' : 1"));
            session.Execute(String.Format(createKeyspaceQuery, "\"KS4\"", "SimpleStrategy", "'replication_factor' : 3"));
            //Let the magic happen
            Thread.Sleep(5000);
            Assert.Greater(cluster.Metadata.GetKeyspaces().Count, initialLength);
            var ks1 = cluster.Metadata.GetKeyspace("ks1");
            Assert.NotNull(ks1);
            Assert.AreEqual(ks1.Replication["replication_factor"], 1);
            var ks2 = cluster.Metadata.GetKeyspace("ks2");
            Assert.NotNull(ks2);
            Assert.AreEqual(ks2.Replication["replication_factor"], 3);
            //GetReplicas should yield the 2 replicas (rf=3 but cluster=2) when the Keyspace is found
            Assert.AreEqual(2, cluster.GetReplicas("ks2", new byte[] {0, 0, 0, 1}).Count);
            var ks3 = cluster.Metadata.GetKeyspace("ks3");
            Assert.NotNull(ks3);
            Assert.AreEqual(ks3.Replication["dc1"], 1);
            Assert.Null(cluster.Metadata.GetKeyspace("ks4"));
            Assert.NotNull(cluster.Metadata.GetKeyspace("KS4"));
        }

        [Test]
        public void MetadataMethodReconnects()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            //The control connection is connected to host 1
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.Address));
            testCluster.StopForce(1);
            Thread.Sleep(10000);

            //The control connection is still connected to host 1
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.Address));
            var t = cluster.Metadata.GetTable("system", "local");
            Assert.NotNull(t);

            //The control connection should be connected to host 2
            Assert.AreEqual(2, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.Address));
        }

        [Test]
        public void HostDownViaMetadataEvents()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var downEventFired = false;
            cluster.Metadata.HostsEvent += (sender, e) =>
            {
                if (e.What == HostsEventArgs.Kind.Down)
                {
                    downEventFired = true;
                }
            };

            //The control connection is connected to host 1
            //All host are up
            Assert.True(cluster.AllHosts().All(h => h.IsUp));
            testCluster.StopForce(2);

            var counter = 0;
            const int maxWait = 100;
            //No query to avoid getting a socket exception
            while (counter++ < maxWait)
            {
                if (cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == 2 && !h.IsUp))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            Assert.True(cluster.AllHosts().Any(h => TestHelper  .GetLastAddressByte(h) == 2 && !h.IsUp));
            Assert.AreNotEqual(counter, maxWait, "Waited but it was never notified via events");
            Assert.True(downEventFired);
        }

        /// <summary>
        /// Starts a cluster with 2 nodes, kills one of them (the one used by the control connection or the other) and checks that the Host Down event was fired.
        /// Then restarts the node and checks that the Host Up event fired.
        /// </summary>
        [TestCase(true, Description = "Using the control connection host")]
        [TestCase(false, Description = "Using the other host")]
        public void MetadataHostsEventTest(bool useControlConnectionHost)
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            var downEventFired = false;
            var upEventFired = false;
            cluster.Metadata.HostsEvent += (sender, e) =>
            {
                if (e.What == HostsEventArgs.Kind.Down)
                {
                    downEventFired = true;
                }
                else
                {
                    upEventFired = true;
                }
            };
            //The host not used by the control connection
            int hostToKill = TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.Address);
            if (!useControlConnectionHost)
            {
                hostToKill = hostToKill == 1 ? 2 : 1;
            }
            testCluster.Stop(hostToKill);
            Thread.Sleep(10000);
            TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
            Assert.True(cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == hostToKill && !h.IsUp));
            Assert.True(downEventFired);
            testCluster.Start(hostToKill);
            Thread.Sleep(20000);
            TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
            Assert.True(cluster.AllHosts().All(h => h.IsConsiderablyUp));
            //When the host of the control connection is used
            //It can result that event UP is not fired as it is not received by the control connection (it reconnected but missed the event) 
            Assert.True(upEventFired || useControlConnectionHost);
        }

        private void CheckPureMetadata(Cluster cluster, ISession session, string tableName, string keyspaceName, TableOptions tableOptions = null)
        {
            // build create table cql
            tableName = TestUtils.GetUniqueTableName().ToLower();
            var columns = new Dictionary
                <string, ColumnTypeCode>
            {
                {"q0uuid", ColumnTypeCode.Uuid},
                {"q1timestamp", ColumnTypeCode.Timestamp},
                {"q2double", ColumnTypeCode.Double},
                {"q3int32", ColumnTypeCode.Int},
                {"q4int64", ColumnTypeCode.Bigint},
                {"q5float", ColumnTypeCode.Float},
                {"q6inet", ColumnTypeCode.Inet},
                {"q7boolean", ColumnTypeCode.Boolean},
                {"q8inet", ColumnTypeCode.Inet},
                {"q9blob", ColumnTypeCode.Blob},
                {"q10varint", ColumnTypeCode.Varint},
                {"q11decimal", ColumnTypeCode.Decimal},
                {"q12list", ColumnTypeCode.List},
                {"q13set", ColumnTypeCode.Set},
                {"q14map", ColumnTypeCode.Map}
                //{"q12counter", Metadata.ColumnTypeCode.Counter}, A table that contains a counter can only contain counters
            };

            var stringBuilder = new StringBuilder(@"CREATE TABLE " + tableName + " (");

            foreach (KeyValuePair<string, ColumnTypeCode> col in columns)
                stringBuilder.Append(col.Key + " " + col.Value +
                          (((col.Value == ColumnTypeCode.List) ||
                            (col.Value == ColumnTypeCode.Set) ||
                            (col.Value == ColumnTypeCode.Map))
                              ? "<int" + (col.Value == ColumnTypeCode.Map ? ",varchar>" : ">")
                              : "") + ", ");

            stringBuilder.Append("PRIMARY KEY(");
            int rowKeys = Randomm.Instance.Next(1, columns.Count - 3);
            for (int i = 0; i < rowKeys; i++)
                stringBuilder.Append(columns.Keys.First(key => key.StartsWith("q" + i.ToString(CultureInfo.InvariantCulture))) + ((i == rowKeys - 1) ? "" : ", "));
            string opt = tableOptions != null ? " WITH " + tableOptions : "";
            stringBuilder.Append("))" + opt + ";");

            QueryTools.ExecuteSyncNonQuery(session, stringBuilder.ToString());
            TestUtils.WaitForSchemaAgreement(session.Cluster);

            var table = cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.AreEqual(tableName, table.Name);
            foreach (var metaCol in table.TableColumns)
            {
                Assert.True(columns.Keys.Contains(metaCol.Name));
                Assert.True(metaCol.TypeCode == columns.First(tpc => tpc.Key == metaCol.Name).Value);
                Assert.True(metaCol.Table == tableName);
                Assert.True(metaCol.Keyspace == (keyspaceName));
            }

            if (tableOptions != null)
            {
                Assert.AreEqual(tableOptions.Comment, table.Options.Comment);
                Assert.AreEqual(tableOptions.ReadRepairChance, table.Options.ReadRepairChance);
                Assert.AreEqual(tableOptions.LocalReadRepairChance, table.Options.LocalReadRepairChance);
                Assert.AreEqual(tableOptions.ReplicateOnWrite, table.Options.replicateOnWrite);
                Assert.AreEqual(tableOptions.GcGraceSeconds, table.Options.GcGraceSeconds);
                Assert.AreEqual(tableOptions.bfFpChance, table.Options.bfFpChance);
                if (tableOptions.Caching == "ALL")
                {
                    //The string returned can be more complete than the provided
                    Assert.That(table.Options.Caching == "ALL" || table.Options.Caching.Contains("ALL"), "Caching returned does not match");
                }
                else
                {
                    Assert.AreEqual(tableOptions.Caching, table.Options.Caching);
                }
                Assert.AreEqual(tableOptions.CompactionOptions, table.Options.CompactionOptions);
                Assert.AreEqual(tableOptions.CompressionParams, table.Options.CompressionParams);
            }
        }

        private void CheckMetadata(string tableName, string keyspaceName, TableOptions tableOptions = null)
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            CheckPureMetadata(cluster, session, tableName, keyspaceName, tableOptions);
        }

        [Test]
        public void CheckSimpleStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;
            bool durableWrites = Randomm.Instance.NextBoolean();
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();

            string strategyClass = ReplicationStrategies.SimpleStrategy;
            int replicationFactor = Randomm.Instance.Next(1, 21);
            session.CreateKeyspace(keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(replicationFactor),
                durableWrites);
            session.ChangeKeyspace(keyspaceName);

            KeyspaceMetadata ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.AreEqual(strategyClass, ksmd.StrategyClass);
            Assert.AreEqual(durableWrites, ksmd.DurableWrites);
            Assert.AreEqual(replicationFactor, ksmd.Replication["replication_factor"]);
        }

        [Test]
        public void CheckNetworkTopologyStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            bool durableWrites = Randomm.Instance.NextBoolean();
            Dictionary<string, int> datacentersReplicationFactors = null;

            string strategyClass = ReplicationStrategies.NetworkTopologyStrategy;
            int dataCentersCount = Randomm.Instance.Next(1, 11);
            datacentersReplicationFactors = new Dictionary<string, int>((int) dataCentersCount);
            for (int i = 0; i < dataCentersCount; i++)
                datacentersReplicationFactors.Add("dc" + i, Randomm.Instance.Next(1, 21));
            session.CreateKeyspace(keyspaceName,
                ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(
                    datacentersReplicationFactors), durableWrites);

            KeyspaceMetadata ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.AreEqual(strategyClass, ksmd.StrategyClass);
            Assert.AreEqual(durableWrites, ksmd.DurableWrites);
            if (datacentersReplicationFactors != null)
                Assert.True(datacentersReplicationFactors.SequenceEqual(ksmd.Replication));
        }

        [Test]
        public void CheckTableMetadata()
        {
            CheckMetadata(TestUtils.GetUniqueTableName(), TestUtils.GetUniqueKeyspaceName());
        }

        [Test]
        public void CheckTableMetadataWithOptions()
        {
            string tableName = TestUtils.GetUniqueTableName();
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();

            CheckMetadata(tableName, keyspaceName,
                tableOptions: new TableOptions("Comment", 0.5, 0.6, false, 42, 0.01, "ALL",
                new SortedDictionary<string, string>
                {
                    {"class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"},
                    {"sstable_size_in_mb", "15"}
                },
                new SortedDictionary<string, string>
                {
                    {"sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor"},
                    {"chunk_length_kb", "128"}
                }));
        }

        [Test]
        public void CheckKeyspaceMetadata()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;

            const string strategyClass = "SimpleStrategy";
            const bool durableWrites = false;
            const int replicationFactor = 1;
            string cql = string.Format(@"
                        CREATE KEYSPACE {0} 
                        WITH replication = {{ 'class' : '{1}', 'replication_factor' : {2} }}
                        AND durable_writes={3};", keyspaceName, strategyClass, 1, durableWrites);
            session.Execute(cql);
            session.ChangeKeyspace(keyspaceName);

            for (var i = 0; i < 10; i++)
            {
                CheckPureMetadata(testCluster.Cluster, session, TestUtils.GetUniqueTableName(), keyspaceName);
            }

            var ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.True(ksmd.DurableWrites == durableWrites);
            Assert.True(ksmd.Replication.First(opt => opt.Key == "replication_factor").Value == replicationFactor);
            Assert.True(ksmd.StrategyClass == strategyClass);
        }

        [Test]
        public void TableMetadataNestedCollectionsTest()
        {
            if (CassandraVersion < Version.Parse("2.1.3"))
            {
                Assert.Ignore("Nested frozen collections are supported in 2.1.3 and above");
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "tbl_nested_cols_meta";
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(String.Format("CREATE TABLE {0} (" +
                                          "id uuid primary key, " +
                                          "map1 map<varchar, frozen<list<timeuuid>>>," +
                                          "map2 map<int, frozen<map<uuid, bigint>>>," +
                                          "list1 list<frozen<map<uuid, int>>>)", tableName));
            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.AreEqual(4, table.TableColumns.Length);
            var map1 = table.TableColumns.First(c => c.Name == "map1");
            Assert.AreEqual(ColumnTypeCode.Map, map1.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(map1.TypeInfo);
            var map1Info = (MapColumnInfo)map1.TypeInfo;
            Assert.True(map1Info.KeyTypeCode == ColumnTypeCode.Varchar || map1Info.KeyTypeCode == ColumnTypeCode.Text,
                "Expected {0} but was {1}", ColumnTypeCode.Varchar, map1Info.KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.List, map1Info.ValueTypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(map1Info.ValueTypeInfo);
            var map1ListInfo = (ListColumnInfo)map1Info.ValueTypeInfo;
            Assert.AreEqual(ColumnTypeCode.Timeuuid, map1ListInfo.ValueTypeCode);

            var map2 = table.TableColumns.First(c => c.Name == "map2");
            Assert.AreEqual(ColumnTypeCode.Map, map2.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(map2.TypeInfo);
            var map2Info = (MapColumnInfo)map2.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.Int, map2Info.KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Map, map2Info.ValueTypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(map2Info.ValueTypeInfo);
            var map2MapInfo = (MapColumnInfo)map2Info.ValueTypeInfo;
            Assert.AreEqual(ColumnTypeCode.Uuid, map2MapInfo.KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, map2MapInfo.ValueTypeCode);

            var list1 = table.TableColumns.First(c => c.Name == "list1");
            Assert.AreEqual(ColumnTypeCode.List, list1.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(list1.TypeInfo);
            var list1Info = (ListColumnInfo)list1.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.Map, list1Info.ValueTypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(list1Info.ValueTypeInfo);
            var list1MapInfo = (MapColumnInfo)list1Info.ValueTypeInfo;
            Assert.AreEqual(ColumnTypeCode.Uuid, list1MapInfo.KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Int, list1MapInfo.ValueTypeCode);
        }

        [Test]
        public void TableMetadataCassandra22Types()
        {
            if (CassandraVersion < Version.Parse("2.2"))
            {
                Assert.Ignore("Date, Time, SmallInt and TinyInt are supported in 2.2 and above");
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "tbl_cass22_types";
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(String.Format("CREATE TABLE {0} (" +
                                          "id uuid primary key, " +
                                          "map1 map<smallint, date>," +
                                          "s smallint," +
                                          "b tinyint," +
                                          "d date," +
                                          "t time)", tableName));
            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.AreEqual(6, table.TableColumns.Length);
            CollectionAssert.AreEqual(table.PartitionKeys, new[] { table.TableColumns.First(c => c.Name == "id") });
            var map1 = table.TableColumns.First(c => c.Name == "map1");
            Assert.AreEqual(ColumnTypeCode.Map, map1.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(map1.TypeInfo);
            var map1Info = (MapColumnInfo)map1.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.SmallInt, map1Info.KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Date, map1Info.ValueTypeCode);

            Assert.AreEqual(ColumnTypeCode.SmallInt, table.TableColumns.First(c => c.Name == "s").TypeCode);
            Assert.AreEqual(ColumnTypeCode.TinyInt, table.TableColumns.First(c => c.Name == "b").TypeCode);
            Assert.AreEqual(ColumnTypeCode.Date, table.TableColumns.First(c => c.Name == "d").TypeCode);
            Assert.AreEqual(ColumnTypeCode.Time, table.TableColumns.First(c => c.Name == "t").TypeCode);
        }

        [Test]
        public void TableMetadata_With_Compact_Storage()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                session.CreateKeyspaceIfNotExists("ks_meta_compac");
                session.Execute("CREATE TABLE ks_meta_compac.tbl5 (id1 uuid, id2 timeuuid, text1 text, PRIMARY KEY (id1, id2)) WITH COMPACT STORAGE");
                session.Execute("CREATE TABLE ks_meta_compac.tbl6 (id uuid, text1 text, text2 text, PRIMARY KEY (id)) WITH COMPACT STORAGE");

                var table = cluster.Metadata
                    .GetKeyspace("ks_meta_compac")
                    .GetTableMetadata("tbl5");
                Assert.NotNull(table);
                Assert.True(table.Options.IsCompactStorage);
                CollectionAssert.AreEquivalent(new[] { "id1", "id2", "text1" }, table.TableColumns.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id1" }, table.PartitionKeys.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id2" }, table.ClusteringKeys.Select(c => c.Item1.Name));
                CollectionAssert.AreEqual(new[] { SortOrder.Ascending }, table.ClusteringKeys.Select(c => c.Item2));
                
                table = cluster.Metadata
                    .GetKeyspace("ks_meta_compac")
                    .GetTableMetadata("tbl6");
                Assert.NotNull(table);
                Assert.True(table.Options.IsCompactStorage);
                CollectionAssert.AreEquivalent(new[] { "id", "text1", "text2" }, table.TableColumns.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id" }, table.PartitionKeys.Select(c => c.Name));
                Assert.AreEqual(0, table.ClusteringKeys.Length);
            }
        }

        /// <summary>
        /// Performs several schema changes and tries to query the newly created keyspaces and tables asap in a multiple node cluster, trying to create a race condition.
        /// </summary>
        [Test]
        public void SchemaAgreementRaceTest()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(3, DefaultMaxClusterCreateRetries, true, false);
            var queries = new[]
            {
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
                "CREATE TABLE ks1.tbl1 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks1.tbl1",
                "SELECT * FROM ks1.tbl1 where id = d54cb06d-d168-45a0-b1b2-9f5c75435d3d",
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
                "CREATE TABLE ks2.tbl2 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl2",
                "SELECT * FROM ks2.tbl2",
                "CREATE TABLE ks2.tbl3 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl3",
                "SELECT * FROM ks2.tbl3",
                "CREATE TABLE ks2.tbl4 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl4",
                "SELECT * FROM ks2.tbl4",
                "SELECT * FROM ks2.tbl4"
            };
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                //warm up the pool
                TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
                foreach (var q in queries)
                {
                    Assert.DoesNotThrow(() => session.Execute(q));
                }
                CollectionAssert.Contains(cluster.Metadata.GetTables("ks2"), "tbl4");
                CollectionAssert.Contains(cluster.Metadata.GetTables("ks1"), "tbl1");
            }
        }

        [Test]
        public void Should_Retrieve_Host_Cassandra_Version()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                CollectionAssert.DoesNotContain(cluster.Metadata.Hosts.Select(h => h.CassandraVersion), null);
            }
        }

        /// Tests that materialized view metadata is being properly retrieved
        /// 
        /// GetMaterializedView_Should_Retrieve_View_Metadata tests that materialized view metadata is being properly populated by the driver.
        /// It first creates a base table with some sample columns, and a materialized view based on those columns. It then verifies the various metadata
        /// associated with the view. 
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is properly populated
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void GetMaterializedView_Should_Retrieve_View_Metadata()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE ks_view_meta.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW ks_view_meta.dailyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY ((game, year, month, day), score, user) WITH CLUSTERING ORDER BY (score DESC)"
            };
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                foreach (var q in queries)
                {
                    session.Execute(q);
                }
                
                var ks = cluster.Metadata.GetKeyspace("ks_view_meta");
                Assert.NotNull(ks);
                var view = ks.GetMaterializedViewMetadata("dailyhigh");
                Assert.NotNull(view);
                Assert.NotNull(view.Options);
                //Value is cached
                var view2 = cluster.Metadata.GetMaterializedView("ks_view_meta", "dailyhigh");
                Assert.AreSame(view, view2);

                Assert.AreEqual("dailyhigh", view.Name);
                Assert.AreEqual(
                    "game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL",
                    view.WhereClause);
                Assert.AreEqual(6, view.TableColumns.Length);

                Assert.AreEqual(new[] { "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta" }, 
                    view.TableColumns.Select(c => c.Keyspace));
                Assert.AreEqual(new[] { "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh" },
                    view.TableColumns.Select(c => c.Table));

                Assert.AreEqual(new[] { "day", "game", "month", "score", "user", "year" }, view.TableColumns.Select(c => c.Name));
                Assert.AreEqual(new[] { ColumnTypeCode.Int, ColumnTypeCode.Varchar, ColumnTypeCode.Int, ColumnTypeCode.Int, ColumnTypeCode.Varchar, 
                    ColumnTypeCode.Int }, view.TableColumns.Select(c => c.TypeCode));
                Assert.AreEqual(new[] { "game", "year", "month", "day" }, view.PartitionKeys.Select(c => c.Name));
                Assert.AreEqual(new[] { "score", "user" }, view.ClusteringKeys.Select(c => c.Item1.Name));
                Assert.AreEqual(new[] { SortOrder.Descending, SortOrder.Ascending }, view.ClusteringKeys.Select(c => c.Item2));
            }
        }

        /// Tests that materialized view metadata with quoted identifiers is being retrieved
        /// 
        /// MaterializedView_Should_Retrieve_View_Metadata_Quoted_Identifiers tests that materialized view metadata with quoated identifiers is being 
        /// properly populated by the driver. It first creates a base table with some sample columns, where these columns have quoted identifers as their name.
        /// It then creates a materialized view based on those columns. It then verifies the various metadata associated with the view. 
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is properly populated
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void MaterializedView_Should_Retrieve_View_Metadata_Quoted_Identifiers()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                @"CREATE TABLE ks_view_meta2.t1 (""theKey"" INT, ""the;Clustering"" INT, ""the Value"" INT, PRIMARY KEY (""theKey"", ""the;Clustering""))",
                @"CREATE MATERIALIZED VIEW ks_view_meta2.mv1 AS SELECT ""theKey"", ""the;Clustering"", ""the Value"" FROM t1 WHERE ""theKey"" IS NOT NULL AND ""the;Clustering"" IS NOT NULL AND ""the Value"" IS NOT NULL PRIMARY KEY (""theKey"", ""the;Clustering"")"
            };
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                foreach (var q in queries)
                {
                    session.Execute(q);
                }

                var ks = cluster.Metadata.GetKeyspace("ks_view_meta2");
                Assert.NotNull(ks);
                var view = ks.GetMaterializedViewMetadata("mv1");
                Assert.NotNull(view);
                Assert.NotNull(view.Options);

                Assert.AreEqual("mv1", view.Name);
                Assert.AreEqual(@"""theKey"" IS NOT NULL AND ""the;Clustering"" IS NOT NULL AND ""the Value"" IS NOT NULL", view.WhereClause);
                Assert.AreEqual(3, view.TableColumns.Length);

                Assert.AreEqual(new[] { "ks_view_meta2", "ks_view_meta2", "ks_view_meta2" }, view.TableColumns.Select(c => c.Keyspace));
                Assert.AreEqual(new[] { "mv1", "mv1", "mv1" }, view.TableColumns.Select(c => c.Table));

                Assert.AreEqual(new[] { "the Value", "the;Clustering", "theKey" }, view.TableColumns.Select(c => c.Name));
                Assert.AreEqual(new[] { ColumnTypeCode.Int, ColumnTypeCode.Int, ColumnTypeCode.Int }, view.TableColumns.Select(c => c.TypeCode));
                Assert.AreEqual(new[] { "theKey" }, view.PartitionKeys.Select(c => c.Name));
                Assert.AreEqual(new[] { "the;Clustering" }, view.ClusteringKeys.Select(c => c.Item1.Name));
                Assert.AreEqual(new[] { SortOrder.Ascending }, view.ClusteringKeys.Select(c => c.Item2));
            }
        }
    }
}
