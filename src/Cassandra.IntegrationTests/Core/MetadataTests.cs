﻿//
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

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class MetadataTests : TestGlobals
    {
        private const int DefaultNodeCount = 1;

        [TestFixtureSetUp]
        public void FixureSetup()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void KeyspacesMetadataAvailableAtStartup()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;

            // Basic status check
            Assert.Greater(cluster.Metadata.GetKeyspaces().Count, 0);
            Assert.NotNull(cluster.Metadata.GetKeyspace("system"));
            Assert.AreEqual("system", cluster.Metadata.GetKeyspace("system").Name);

            //Not existent tables return null
            Assert.Null(cluster.Metadata.GetKeyspace("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12)));
            Assert.Null(cluster.Metadata.GetTable("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12), "nonExistentTable_" + Randomm.RandomAlphaNum(12)));
            Assert.Null(cluster.Metadata.GetTable("system", "nonExistentTable_" + Randomm.RandomAlphaNum(12)));

            //Case sensitive
            Assert.Null(cluster.Metadata.GetKeyspace("SYSTEM"));
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
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
            testCluster.StopForce(1);
            Thread.Sleep(10000);

            //The control connection is still connected to host 1
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
            var t = cluster.Metadata.GetTable("system", "schema_columnfamilies");
            Assert.NotNull(t);

            //The control connection should be connected to host 2
            Assert.AreEqual(2, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
        }

        [Test]
        public void HostDownViaMetadataEvents()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;

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
            Assert.True(cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == 2 && !h.IsUp));
            Assert.AreNotEqual(counter, maxWait, "Waited but it was never notified via events");
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
            var rowSet = session.Execute(cql);
            session.WaitForSchemaAgreement(rowSet);
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

        [Test, TestCassandraVersion(2,1)]
        public void UdtMetadataTest()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string cqlType1 = "CREATE TYPE phone (alias text, number text)";
            const string cqlType2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<frozen<phone>>)";
            const string cqlTable = "CREATE TABLE user (id int PRIMARY KEY, addr frozen<address>, main_phone frozen<phone>)";

            session.Execute(cqlType1);
            session.Execute(cqlType2);
            session.Execute(cqlTable);
            var table = cluster.Metadata.GetTable(keyspaceName, "user");
            Assert.AreEqual(3, table.TableColumns.Length);
            var udtColumn = table.TableColumns.First(c => c.Name == "addr");
            Assert.AreEqual(ColumnTypeCode.Udt, udtColumn.TypeCode);
            Assert.IsInstanceOf<UdtColumnInfo>(udtColumn.TypeInfo);
            var udtInfo = (UdtColumnInfo) udtColumn.TypeInfo;
            Assert.AreEqual(3, udtInfo.Fields.Count);
            Assert.AreEqual(keyspaceName + ".address", udtInfo.Name);

            var phoneDefinition = cluster.Metadata.GetUdtDefinition(keyspaceName, "phone");
            Assert.AreEqual(keyspaceName + ".phone", phoneDefinition.Name);
            Assert.AreEqual(2, phoneDefinition.Fields.Count);

            var addressDefinition = cluster.Metadata.GetUdtDefinition(keyspaceName, "address");
            Assert.AreEqual(keyspaceName + ".address", addressDefinition.Name);
            Assert.AreEqual("street,ZIP,phones", String.Join(",", addressDefinition.Fields.Select(f => f.Name)));
            Assert.AreEqual(ColumnTypeCode.Int, addressDefinition.Fields.First(f => f.Name == "ZIP").TypeCode);
            var phoneSet = addressDefinition.Fields.First(f => f.Name == "phones");
            Assert.AreEqual(ColumnTypeCode.Set, phoneSet.TypeCode);
            var phoneSetSubType = (SetColumnInfo) phoneSet.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.Udt, phoneSetSubType.KeyTypeCode);
            Assert.AreEqual(2, ((UdtColumnInfo) phoneSetSubType.KeyTypeInfo).Fields.Count);

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, "user");
            Assert.AreEqual(3, tableMetadata.TableColumns.Count());
            Assert.AreEqual(ColumnTypeCode.Udt, tableMetadata.TableColumns.First(c => c.Name == "addr").TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void TupleMetadataTest()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            string cqlTable1 = "CREATE TABLE " + tableName + " (id int PRIMARY KEY, phone frozen<tuple<uuid, text, int>>, achievements list<frozen<tuple<text,int>>>)";

            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cqlTable1);

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.AreEqual(3, tableMetadata.TableColumns.Count());
        }

        [Test]
        public void TableMetadataCompositePartitionKeyTest()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            string tableName1 = TestUtils.GetUniqueTableName().ToLower();
            string cql = "CREATE TABLE " + tableName1 + " ( " +
                    @"a text,
                    b int,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b), c))";
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cql);

            session.Execute("INSERT INTO " + tableName1 + " (a, b, c, d) VALUES ('1', 2, 3, 4)");
            var rs = session.Execute("select * from " + tableName1);
            Assert.True(rs.GetRows().Count() == 1);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName1);
            Assert.True(table.TableColumns.Count() == 4);

            string tableName2 = TestUtils.GetUniqueTableName().ToLower();
            cql = "CREATE TABLE " + tableName2 + " ( " +
                    @"a text,
                    b text,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b, c)))";
            session.WaitForSchemaAgreement(session.Execute(cql));

            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName2);
            Assert.True(table.TableColumns.Count() == 4);

            string tableName3 = TestUtils.GetUniqueTableName().ToLower();
            cql = "CREATE TABLE " + tableName3 + " ( " +
                    @"a text,
                    b text,
                    c timestamp,
                    d int,
                    PRIMARY KEY (a, b, c))";
            session.Execute(cql);

            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName3);
            Assert.True(table.TableColumns.Count() == 4);
        }

        [Test]
        public void TableMetadataClusteringOrderTest()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            var cql = "CREATE TABLE " + tableName + " (" +
                    @"a text,
                    b int,
                    c text,
                    d text,
                    f text,
                    g text,
                    h timestamp,
                    PRIMARY KEY ((a, b), c, d)
                    ) WITH CLUSTERING ORDER BY (c ASC, d DESC);
                ";
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cql);

            session.Execute("INSERT INTO " + tableName + " (a, b, c, d) VALUES ('1', 2, '3', '4')");
            var rs = session.Execute("select * from " + tableName);
            Assert.True(rs.GetRows().Count() == 1);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);
            Assert.NotNull(table);
            Assert.True(table.TableColumns.Count() == 7);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void TableMetadataCollectionsSecondaryIndexTest()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            string tableName = "products";
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      description text,
                      price int,
                      categories set<text>,
                      features map<text, text>)";
            session.Execute(cql);
            cql = "CREATE INDEX cat_index ON " + tableName + "(categories)";
            session.Execute(cql);
            cql = "CREATE INDEX feat_key_index ON " + tableName + "(KEYS(features))";
            session.Execute(cql);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.AreEqual(5, table.TableColumns.Count());
        }

        [Test]
        public void TableMetadataAllTypesTest()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(String.Format(TestUtils.CreateTableAllTypes, tableName));

            Assert.Null(cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata("tbl_does_not_exists"));

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.NotNull(table);
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "id"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "ascii_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "text_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "int_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "bigint_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "float_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "double_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "decimal_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "blob_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "boolean_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "timestamp_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "inet_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "timeuuid_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "map_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "list_sample"));
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "set_sample"));

            var tableByAll = cluster.Metadata.GetKeyspace(keyspaceName).GetTablesMetadata().First(t => t.Name == tableName);
            Assert.NotNull(tableByAll);
            Assert.AreEqual(table.TableColumns.Length, tableByAll.TableColumns.Length);

            var columnLength = table.TableColumns.Length;
            //Alter table and check for changes
            session.Execute(String.Format("ALTER TABLE {0} ADD added_col int", tableName));
            Thread.Sleep(1000);
            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName);
            Assert.AreEqual(columnLength + 1, table.TableColumns.Length);
            Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "added_col"));
        }
    }
}
