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
    public class MetadataTests
    {
        private const string Keyspace = "tester";
        private Cluster Cluster;
        private ISession Session;

        [TestFixtureSetUp]
        public void SetFixture()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }

        [Test]
        public void KeyspacesMetadataAvailableAtStartup()
        {
            var clusterInfo = TestUtils.CcmSetup(1, null, null, 0, false);
            try
            {
                var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
                cluster.Connect();
                Assert.Greater(cluster.Metadata.GetKeyspaces().Count, 0);
                Assert.NotNull(cluster.Metadata.GetKeyspace("system"));
                Assert.AreEqual("system", cluster.Metadata.GetKeyspace("system").Name);
                //Not existent tables return null
                Assert.Null(cluster.Metadata.GetKeyspace("ks_not_existent"));
                Assert.Null(cluster.Metadata.GetTable("ks_not_existent", "tbl1"));
                Assert.Null(cluster.Metadata.GetTable("system", "tbl1"));
                //Case sensitive
                Assert.Null(cluster.Metadata.GetKeyspace("SYSTEM"));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        /// When there is a change in schema, it should be received via ControlConnection
        /// </summary>
        [Test]
        public void KeyspacesMetadataUpToDateViaCassandraEvents()
        {
            var clusterInfo = TestUtils.CcmSetup(2, null, null, 0, false);
            try
            {
                var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
                var session = cluster.Connect();
                var initialLength = cluster.Metadata.GetKeyspaces().Count;
                Assert.Greater(initialLength, 0);

                //GetReplicas should yield the primary replica when the Keyspace is not found
                Assert.AreEqual(1, cluster.GetReplicas("ks2", new byte[]{0, 0, 0, 1}).Count);

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
                Assert.AreEqual(2, cluster.GetReplicas("ks2", new byte[] { 0, 0, 0, 1 }).Count);
                var ks3 = cluster.Metadata.GetKeyspace("ks3");
                Assert.NotNull(ks3);
                Assert.AreEqual(ks3.Replication["dc1"], 1);
                Assert.Null(cluster.Metadata.GetKeyspace("ks4"));
                Assert.NotNull(cluster.Metadata.GetKeyspace("KS4"));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void MetadataMethodReconnects()
        {
            var clusterInfo = TestUtils.CcmSetup(2, null, null, 0, false);
            try
            {
                var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
                var session = cluster.Connect();
                //The control connection is connected to host 1
                Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
                TestUtils.CcmStopForce(clusterInfo, 1);
                Thread.Sleep(10000);
                //The control connection is still connected to host 1
                Assert.AreEqual(1, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
                //it should reconnect
                var t = cluster.Metadata.GetTable("system", "schema_columnfamilies");
                Assert.NotNull(t);
                //The control connection should be connected to host 2
                Assert.AreEqual(2, TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.BindAddress));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void HostDownViaMetadataEvents()
        {
            var clusterInfo = TestUtils.CcmSetup(2, null, null, 0, false);
            try
            {
                var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
                var session = cluster.Connect();
                //The control connection is connected to host 1
                //All host are up
                Assert.True(cluster.AllHosts().All(h => h.IsUp));
                
                TestUtils.CcmStopForce(clusterInfo, 2);

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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        private void CheckPureMetadata(string tableName = null, string keyspaceName = null, TableOptions tableOptions = null)
        {
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

            tableName = tableName ?? "table" + Guid.NewGuid().ToString("N");
            var sb = new StringBuilder(@"CREATE TABLE " + tableName + " (");

            foreach (KeyValuePair<string, ColumnTypeCode> col in columns)
                sb.Append(col.Key + " " + col.Value +
                          (((col.Value == ColumnTypeCode.List) ||
                            (col.Value == ColumnTypeCode.Set) ||
                            (col.Value == ColumnTypeCode.Map))
                               ? "<int" + (col.Value == ColumnTypeCode.Map ? ",varchar>" : ">")
                               : "") + ", ");

            sb.Append("PRIMARY KEY(");
            int rowKeys = Randomm.Instance.Next(1, columns.Count - 3);

            for (int i = 0; i < rowKeys; i++)
                sb.Append(columns.Keys.First(key => key.StartsWith("q" + i.ToString(CultureInfo.InvariantCulture))) + ((i == rowKeys - 1) ? "" : ", "));

            string opt = tableOptions != null ? " WITH " + tableOptions : "";
            sb.Append("))" + opt + ";");

            QueryTools.ExecuteSyncNonQuery(Session, sb.ToString());
            TestUtils.WaitForSchemaAgreement(Session.Cluster);

            var table = Cluster.Metadata.GetTable(keyspaceName ?? Keyspace, tableName);
            Assert.AreEqual(tableName, table.Name);
            foreach (var metaCol in table.TableColumns)
            {
                Assert.True(columns.Keys.Contains(metaCol.Name));
                Assert.True(metaCol.TypeCode == columns.First(tpc => tpc.Key == metaCol.Name).Value);
                Assert.True(metaCol.Table == tableName);
                Assert.True(metaCol.Keyspace == (keyspaceName ?? Keyspace));
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

        private void CheckMetadata(string tableName = null, string keyspaceName = null, TableOptions tableOptions = null)
        {
            var clusterInfo = TestUtils.CcmSetup(2);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);

                CheckPureMetadata(tableName, keyspaceName, tableOptions);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        private void CreateKeyspaceWithPropertiesTest(string strategyClass)
        {
            var clusterInfo = TestUtils.CcmSetup(2);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;

                bool durable_writes = Randomm.Instance.NextBoolean();

                int? replication_factor = null;
                int? data_centers_count = null;
                Dictionary<string, int> datacenters_replication_factors = null;

                if (strategyClass == ReplicationStrategies.SimpleStrategy)
                {
                    replication_factor = Randomm.Instance.Next(1, 21);
                    Session.CreateKeyspaceIfNotExists(Keyspace,
                                                      ReplicationStrategies.CreateSimpleStrategyReplicationProperty((int) replication_factor),
                                                      durable_writes);
                    Session.ChangeKeyspace(Keyspace);
                }
                else if (strategyClass == ReplicationStrategies.NetworkTopologyStrategy)
                {
                    data_centers_count = Randomm.Instance.Next(1, 11);
                    datacenters_replication_factors = new Dictionary<string, int>((int) data_centers_count);
                    for (int i = 0; i < data_centers_count; i++)
                        datacenters_replication_factors.Add("dc" + i, Randomm.Instance.Next(1, 21));
                    Session.CreateKeyspaceIfNotExists(Keyspace,
                                                      ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(
                                                          datacenters_replication_factors), durable_writes);
                }

                KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(Keyspace);
                Assert.AreEqual(strategyClass, ksmd.StrategyClass);
                Assert.AreEqual(durable_writes, ksmd.DurableWrites);
                if (replication_factor != null)
                    Assert.AreEqual(replication_factor, ksmd.Replication["replication_factor"]);
                if (datacenters_replication_factors != null)
                    Assert.True(datacenters_replication_factors.SequenceEqual(ksmd.Replication));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }


        [Test]
        public void CheckSimpleStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.SimpleStrategy);
        }

        [Test]
        public void CheckNetworkTopologyStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.NetworkTopologyStrategy);
        }

        [Test]
        public void CheckTableMetadata()
        {
            CheckMetadata();
        }

        [Test]
        public void CheckTableMetadataWithOptions()
        {
            CheckMetadata(tableOptions: new TableOptions("Comment", 0.5, 0.6, false, 42, 0.01, "ALL",
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
            var clusterInfo = TestUtils.CcmSetup(2);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);

                var ksName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
                const string strategyClass = "SimpleStrategy";
                const bool durableWrites = false;
                const int replicationFactor = 1;
                Session.WaitForSchemaAgreement(
                    Session.Execute(string.Format(@"
                        CREATE KEYSPACE {0} 
                        WITH replication = {{ 'class' : '{1}', 'replication_factor' : {2} }}
                        AND durable_writes={3};" , ksName, strategyClass, 1, durableWrites))
                );
                Session.ChangeKeyspace(ksName);


                for (var i = 0; i < 10; i++)
                {
                    CheckPureMetadata("table" + Guid.NewGuid().ToString("N"), ksName);
                }

                var ksmd = Cluster.Metadata.GetKeyspace(ksName);
                Assert.True(ksmd.DurableWrites == durableWrites);
                Assert.True(ksmd.Replication.First(opt => opt.Key == "replication_factor").Value == replicationFactor);
                Assert.True(ksmd.StrategyClass == strategyClass);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void UdtMetadataTest()
        {
            if (Options.Default.CassandraVersion < new Version(2, 1))
            {
                Assert.Ignore("Test suitable to be run against Cassandra 2.1 or above");
            }
            const string cqlType1 = "CREATE TYPE phone (alias text, number text)";
            const string cqlType2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<frozen<phone>>)";
            const string cqlTable = "CREATE TABLE user (id int PRIMARY KEY, addr frozen<address>, main_phone frozen<phone>)";
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                Session.Execute(cqlType1);
                Session.Execute(cqlType2);
                Session.Execute(cqlTable);
                var table = Cluster.Metadata.GetTable(Keyspace, "user");
                Assert.AreEqual(3, table.TableColumns.Length);
                var udtColumn = table.TableColumns.First(c => c.Name == "addr");
                Assert.AreEqual(ColumnTypeCode.Udt, udtColumn.TypeCode);
                Assert.IsInstanceOf<UdtColumnInfo>(udtColumn.TypeInfo);
                var udtInfo = (UdtColumnInfo)udtColumn.TypeInfo;
                Assert.AreEqual(3, udtInfo.Fields.Count);
                Assert.AreEqual(Keyspace + ".address", udtInfo.Name);

                var phoneDefinition = Cluster.Metadata.GetUdtDefinition(Keyspace, "phone");
                Assert.AreEqual(Keyspace + ".phone", phoneDefinition.Name);
                Assert.AreEqual(2, phoneDefinition.Fields.Count);

                var addressDefinition = Cluster.Metadata.GetUdtDefinition(Keyspace, "address");
                Assert.AreEqual(Keyspace + ".address", addressDefinition.Name);
                Assert.AreEqual("street,ZIP,phones", String.Join(",", addressDefinition.Fields.Select(f => f.Name)));
                Assert.AreEqual(ColumnTypeCode.Int, addressDefinition.Fields.First(f => f.Name == "ZIP").TypeCode);
                var phoneSet = addressDefinition.Fields.First(f => f.Name == "phones");
                Assert.AreEqual(ColumnTypeCode.Set, phoneSet.TypeCode);
                var phoneSetSubType = (SetColumnInfo)phoneSet.TypeInfo;
                Assert.AreEqual(ColumnTypeCode.Udt, phoneSetSubType.KeyTypeCode);
                Assert.AreEqual(2, ((UdtColumnInfo)phoneSetSubType.KeyTypeInfo).Fields.Count);

                var tableMetadata = Cluster.Metadata.GetTable(Keyspace, "user");
                Assert.AreEqual(3, tableMetadata.TableColumns.Count());
                Assert.AreEqual(ColumnTypeCode.Udt, tableMetadata.TableColumns.First(c => c.Name == "addr").TypeCode);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TupleMetadataTest()
        {
            if (Options.Default.CassandraVersion < new Version(2, 1))
            {
                Assert.Ignore("Test suitable to be run against Cassandra 2.1 or above");
            }
            const string cqlTable1 = "CREATE TABLE users_tuples (id int PRIMARY KEY, phone frozen<tuple<uuid, text, int>>, achievements list<frozen<tuple<text,int>>>)";
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                Session.Execute(cqlTable1);

                var tableMetadata = Cluster.Metadata.GetTable(Keyspace, "users_tuples");
                Assert.AreEqual(3, tableMetadata.TableColumns.Count());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TableMetadataCompositePartitionKeyTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                string cql = @"
                    CREATE TABLE sample_composite_partition1 (
                    a text,
                    b int,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b), c))";
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                Session.Execute(cql);

                Session.Execute("INSERT INTO sample_composite_partition1 (a, b, c, d) VALUES ('1', 2, 3, 4)");
                var rs = Session.Execute("select * from sample_composite_partition1");
                Assert.True(rs.GetRows().Count() == 1);

                var table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata("sample_composite_partition1");
                Assert.True(table.TableColumns.Count() == 4);


                cql = @"
                    CREATE TABLE sample_composite_partition2 (
                    a text,
                    b text,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b, c)))";
                Session.WaitForSchemaAgreement(Session.Execute(cql));

                table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata("sample_composite_partition2");
                Assert.True(table.TableColumns.Count() == 4);


                cql = @"
                    CREATE TABLE sample_composite_clusteringkey (
                    a text,
                    b text,
                    c timestamp,
                    d int,
                    PRIMARY KEY (a, b, c))";
                Session.Execute(cql);

                table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata("sample_composite_clusteringkey");
                Assert.True(table.TableColumns.Count() == 4);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TableMetadataClusteringOrderTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;
                var cql = @"
                    CREATE TABLE sample_clustering_order1 (
                    a text,
                    b int,
                    c text,
                    d text,
                    f text,
                    g text,
                    h timestamp,
                    PRIMARY KEY ((a, b), c, d)
                    ) WITH CLUSTERING ORDER BY (c ASC, d DESC);
                ";
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                Session.Execute(cql);

                Session.Execute("INSERT INTO sample_clustering_order1 (a, b, c, d) VALUES ('1', 2, '3', '4')");
                var rs = Session.Execute("select * from sample_clustering_order1");
                Assert.True(rs.GetRows().Count() == 1);

                var table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata("sample_clustering_order1");
                Assert.True(table.TableColumns.Count() == 7);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TableMetadataCollectionsSecondaryIndexTest()
        {
            if (Options.Default.CassandraVersion < new Version(2, 1))
            {
                Assert.Ignore("Test suitable to be run against Cassandra 2.1 or above");
            }
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;

                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);

                var cql = @"
                CREATE TABLE products (
                      id int PRIMARY KEY,
                      description text,
                      price int,
                      categories set<text>,
                      features map<text, text>)";
                Session.Execute(cql);
                cql = "CREATE INDEX cat_index ON products(categories)";
                Session.Execute(cql);
                cql = "CREATE INDEX feat_key_index ON products(KEYS(features))";
                Session.Execute(cql);


                var table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata("products");

                Assert.AreEqual(5, table.TableColumns.Count());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TableMetadataAllTypesTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                Session = clusterInfo.Session;
                Cluster = clusterInfo.Cluster;

                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);

                const string tableName = "sample_metadata_alltypes";

                Session.Execute(String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, tableName));

                Assert.Null(Cluster.Metadata
                                   .GetKeyspace(Keyspace)
                                   .GetTableMetadata("tbl_does_not_exists"));

                var table = Cluster.Metadata
                                   .GetKeyspace(Keyspace)
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

                var tableByAll = Cluster.Metadata.GetKeyspace(Keyspace).GetTablesMetadata().First(t => t.Name == tableName);
                Assert.NotNull(tableByAll);
                Assert.AreEqual(table.TableColumns.Length, tableByAll.TableColumns.Length);

                var columnLength = table.TableColumns.Length;
                //Alter table and check for changes
                Session.Execute(String.Format("ALTER TABLE {0} ADD added_col int", tableName));
                Thread.Sleep(1000);
                table = Cluster.Metadata
                    .GetKeyspace(Keyspace)
                    .GetTableMetadata(tableName);
                Assert.AreEqual(columnLength + 1, table.TableColumns.Length);
                Assert.AreEqual(1, table.TableColumns.Count(c => c.Name == "added_col"));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
