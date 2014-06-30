//
//      Copyright (C) 2012 DataStax Inc.
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
        private Cluster Cluster;
        private string Keyspace = "tester";
        private ISession Session;

        [TestFixtureSetUp]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
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

            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, sb.ToString())
                );

            var table = Cluster.Metadata.GetTable(keyspaceName ?? Keyspace, tableName);
            Assert.AreEqual(tableName, table.Name);
            foreach (TableColumn metaCol in table.TableColumns)
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
            const string cqlType1 = "CREATE TYPE phone (alias text, number text)";
            const string cqlType2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<phone>)";
            const string cqlTable = "CREATE TABLE user (id int PRIMARY KEY, addr address, main_phone phone)";
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
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void CompositePartitionKeyMetadata()
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
                Session.WaitForSchemaAgreement(Session.Execute(cql));

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
                Session.WaitForSchemaAgreement(Session.Execute(cql));

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
    }
}
