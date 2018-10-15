using System;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tasks;
using NUnit.Framework;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class SchemaMetadataTests : SharedClusterTest
    {
        [Test]
        public void KeyspacesMetadataAvailableAtStartup()
        {
            var cluster = GetNewCluster();
            // Basic status check
            Assert.Greater(cluster.Metadata.GetKeyspaces().Count, 0);
            Assert.NotNull(cluster.Metadata.GetKeyspace("system"));
            Assert.AreEqual("system", cluster.Metadata.GetKeyspace("system").Name);

            Assert.NotNull(cluster.Metadata.GetKeyspace("system").AsCqlQuery());

            //Not existent tables return null
            Assert.Null(cluster.Metadata.GetKeyspace("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12)));
            Assert.Null(cluster.Metadata.GetTable("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12), "nonExistentTable_" + Randomm.RandomAlphaNum(12)));
            Assert.Null(cluster.Metadata.GetTable("system", "nonExistentTable_" + Randomm.RandomAlphaNum(12)));

            //Case sensitive
            Assert.Null(cluster.Metadata.GetKeyspace("SYSTEM"));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void UdtMetadataTest()
        {
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
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
            var udtInfo = (UdtColumnInfo)udtColumn.TypeInfo;
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
            var phoneSetSubType = (SetColumnInfo)phoneSet.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.Udt, phoneSetSubType.KeyTypeCode);
            Assert.AreEqual(2, ((UdtColumnInfo)phoneSetSubType.KeyTypeInfo).Fields.Count);

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, "user");
            Assert.AreEqual(3, tableMetadata.TableColumns.Count());
            Assert.AreEqual(ColumnTypeCode.Udt, tableMetadata.TableColumns.First(c => c.Name == "addr").TypeCode);
        }

        [Test]
        public void Custom_MetadataTest()
        {
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string typeName1 = "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                                     "s=>org.apache.cassandra.db.marshal.UTF8Type," +
                                     "i=>org.apache.cassandra.db.marshal.Int32Type)";
            const string typeName2 = "org.apache.cassandra.db.marshal.CompositeType(" +
                                     "org.apache.cassandra.db.marshal.UTF8Type," +
                                     "org.apache.cassandra.db.marshal.Int32Type)";
            session.Execute("CREATE TABLE tbl_custom (id int PRIMARY KEY, " +
                            "c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)', " +
                            "c2 'CompositeType(UTF8Type, Int32Type)')");

            var table = cluster.Metadata.GetTable(keyspaceName, "tbl_custom");
            Assert.AreEqual(3, table.TableColumns.Length);
            var c1 = table.TableColumns.First(c => c.Name == "c1");
            Assert.AreEqual(ColumnTypeCode.Custom, c1.TypeCode);
            var typeInfo1 = (CustomColumnInfo)c1.TypeInfo;
            Assert.AreEqual("tbl_custom", c1.Table);
            Assert.AreEqual(keyspaceName, c1.Keyspace);
            Assert.IsFalse(c1.IsFrozen);
            Assert.IsFalse(c1.IsReversed);
            Assert.AreEqual(typeName1, typeInfo1.CustomTypeName);
            var c2 = table.TableColumns.First(c => c.Name == "c2");
            Assert.AreEqual(ColumnTypeCode.Custom, c2.TypeCode);
            Assert.AreEqual("tbl_custom", c2.Table);
            Assert.AreEqual(keyspaceName, c2.Keyspace);
            Assert.IsFalse(c2.IsFrozen);
            Assert.IsFalse(c2.IsReversed);
            var typeInfo2 = (CustomColumnInfo)c2.TypeInfo;
            Assert.AreEqual(typeName2, typeInfo2.CustomTypeName);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void Udt_Case_Sensitive_Metadata_Test()
        {
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string cqlType = "CREATE TYPE \"MyUdt\" (key1 text, key2 text)";
            const string cqlTable = "CREATE TABLE \"MyTable\" (id int PRIMARY KEY, value frozen<\"MyUdt\">)";

            session.Execute(cqlType);
            session.Execute(cqlTable);
            var table = cluster.Metadata.GetTable(keyspaceName, "MyTable");
            Assert.AreEqual(2, table.TableColumns.Length);
            var udtColumn = table.TableColumns.First(c => c.Name == "value");
            Assert.AreEqual(ColumnTypeCode.Udt, udtColumn.TypeCode);
            Assert.IsInstanceOf<UdtColumnInfo>(udtColumn.TypeInfo);
            var udtInfo = (UdtColumnInfo)udtColumn.TypeInfo;
            Assert.AreEqual(2, udtInfo.Fields.Count);
            Assert.AreEqual(keyspaceName + ".MyUdt", udtInfo.Name);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void TupleMetadataTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cqlTable1 = "CREATE TABLE " + tableName + " (id int PRIMARY KEY, phone frozen<tuple<uuid, text, int>>, achievements list<frozen<tuple<text,int>>>)";

            var cluster = GetNewCluster();
            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cqlTable1);

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.AreEqual(3, tableMetadata.TableColumns.Count());
        }

        [Test]
        public void TableMetadataCompositePartitionKeyTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName1 = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();

            var cql = "CREATE TABLE " + tableName1 + " ( " +
                    @"b int,
                    a text,
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
            Assert.AreEqual(2, table.PartitionKeys.Length);
            Assert.AreEqual("a, b", String.Join(", ", table.PartitionKeys.Select(p => p.Name)));

            string tableName2 = TestUtils.GetUniqueTableName().ToLower();
            cql = "CREATE TABLE " + tableName2 + " ( " +
                    @"a text,
                    b text,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b, c)))";
            session.Execute(cql);

            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName2);
            Assert.True(table.TableColumns.Count() == 4);
            Assert.AreEqual("a, b, c", String.Join(", ", table.PartitionKeys.Select(p => p.Name)));

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
            //Just 1 partition key
            Assert.AreEqual("a", String.Join(", ", table.PartitionKeys.Select(p => p.Name)));
        }

        [Test]
        public void TableMetadataClusteringOrderTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();

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
            Assert.AreEqual(7, table.TableColumns.Length);
            CollectionAssert.AreEqual(new[] { "a", "b" }, table.PartitionKeys.Select(p => p.Name));
            CollectionAssert.AreEqual(new [] { "a", "b"}, table.TableColumns
                .Where(c => c.KeyType == KeyType.Partition)
                .Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "c", "d" }, table.ClusteringKeys.Select(c => c.Item1.Name));
            CollectionAssert.AreEqual(new[] { SortOrder.Ascending, SortOrder.Descending }, table.ClusteringKeys.Select(c => c.Item2));
            CollectionAssert.AreEqual(new[] { "c", "d" }, table.TableColumns
                 .Where(c => c.KeyType == KeyType.Clustering)
                 .Select(c => c.Name));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void TableMetadataCollectionsSecondaryIndexTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "products";
            var cluster = GetNewCluster();
            var session = cluster.Connect();
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

            Assert.AreEqual(2, table.Indexes.Count);

            var catIndex = table.Indexes["cat_index"];
            Assert.AreEqual("cat_index", catIndex.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, catIndex.Kind);
            Assert.AreEqual("values(categories)", catIndex.Target);
            Assert.NotNull(catIndex.Options);
            var featIndex = table.Indexes["feat_key_index"];
            Assert.AreEqual("feat_key_index", featIndex.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, featIndex.Kind);
            Assert.AreEqual("keys(features)", featIndex.Target);
            Assert.NotNull(featIndex.Options);

            Assert.AreEqual(5, table.TableColumns.Count());
        }

        [Test]
        public void TableMetadataAllTypesTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();
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

        [Test]
        public void GetTableAsync_With_Keyspace_And_Table_Not_Found()
        {
            var cluster = GetNewCluster();
            cluster.Connect();
            var t = cluster.Metadata.GetTableAsync("ks_does_not_exist", "t1");
            var table = TaskHelper.WaitToComplete(t);
            Assert.Null(table);
            t = cluster.Metadata.GetTableAsync("system", "table_does_not_exist");
            table = TaskHelper.WaitToComplete(t);
            Assert.Null(table);
        }

        /// Tests that materialized view metadata is being updated
        /// 
        /// GetMaterializedView_Should_Refresh_View_Metadata_Via_Events tests that materialized view metadata is being properly updated by the driver
        /// after a change to the view, via schema change events. It first creates a base table with some sample columns, and a materialized view based on 
        /// those columns. It then verifies verifies that the original compaction strategy was "STCS". It then changes the compaction strategy for the view
        /// to "LCS" and verifies that the view metadata was updated correctly.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is updated correctly
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void GetMaterializedView_Should_Refresh_View_Metadata_Via_Events()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE ks_view_meta3.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW ks_view_meta3.monthlyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL PRIMARY KEY ((game, year, month), score, user, day) WITH CLUSTERING ORDER BY (score DESC) AND compaction = { 'class' : 'SizeTieredCompactionStrategy' }"
            };
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            foreach (var q in queries)
            {
                session.Execute(q);
            }
            var view = cluster.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh");
            Assert.NotNull(view);
            StringAssert.Contains("SizeTieredCompactionStrategy", view.Options.CompactionOptions["class"]);

            const string alterQuery = "ALTER MATERIALIZED VIEW ks_view_meta3.monthlyhigh WITH compaction = { 'class' : 'LeveledCompactionStrategy' }";
            session.Execute(alterQuery);
            //Wait for event
            Thread.Sleep(5000);
            view = cluster.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh");
            StringAssert.Contains("LeveledCompactionStrategy", view.Options.CompactionOptions["class"]);

            const string dropQuery = "DROP MATERIALIZED VIEW ks_view_meta3.monthlyhigh";
            session.Execute(dropQuery);
            //Wait for event
            Thread.Sleep(5000);
            Assert.Null(cluster.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh"));
        }

        /// Tests that materialized view metadata is updated from base table addition changes
        /// 
        /// MaterializedView_Base_Table_Column_Addition tests that materialized view metadata is being updated when there is a table alteration in the base
        /// table for the view, where a new column is added. It first creates a base table with some sample columns, and two materialized views based on 
        /// those columns: one which targets specific columns and the other which targets all columns. It then alters the base table to add a new column 
        /// "fouls". It then verifies that the update is propagated to the table metadata and the view metadata which targets all columns. It finally 
        /// verfies that the view which does not target all the base columns is not affected by this table change.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is updated due to base table changes
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void MaterializedView_Base_Table_Column_Addition()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE ks_view_meta4.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW ks_view_meta4.dailyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY ((game, year, month, day), score, user) WITH CLUSTERING ORDER BY (score DESC)",
                "CREATE MATERIALIZED VIEW ks_view_meta4.alltimehigh AS SELECT * FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY (game, year, month, day, score, user) WITH CLUSTERING ORDER BY (score DESC)"
            };
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            foreach (var q in queries)
            {
                session.Execute(q);
            }

            var ks = cluster.Metadata.GetKeyspace("ks_view_meta4");
            Assert.NotNull(ks);
            var dailyView = ks.GetMaterializedViewMetadata("dailyhigh");
            Assert.NotNull(dailyView);
            Assert.NotNull(dailyView.Options);
            var alltimeView = ks.GetMaterializedViewMetadata("alltimehigh");
            Assert.NotNull(alltimeView);
            Assert.NotNull(alltimeView.Options);

            session.Execute("ALTER TABLE ks_view_meta4.scores ADD fouls INT");
            //Wait for event
            Thread.Sleep(5000);
            Assert.NotNull(cluster.Metadata.GetKeyspace("ks_view_meta4").GetTableMetadata("scores").ColumnsByName["fouls"]);

            alltimeView = cluster.Metadata.GetMaterializedView("ks_view_meta4", "alltimehigh");
            var foulMeta = alltimeView.ColumnsByName["fouls"];
            Assert.NotNull(foulMeta);
            Assert.AreEqual(ColumnTypeCode.Int, foulMeta.TypeCode);

            dailyView = cluster.Metadata.GetMaterializedView("ks_view_meta4", "dailyhigh");
            Assert.IsFalse(dailyView.TableColumns.Contains(foulMeta));
        }

        /// Tests that multiple secondary indexes are supported per column
        /// 
        /// MultipleSecondaryIndexTest tests that multiple secondary indexes can be created on the same column, and the driver
        /// metadata is updated appropriately. It first creates a table with a map column to be used by the secondary index.
        /// It then proceeds to create two secondary indexes on the same column: one for the keys of the map and another for
        /// the values of the map. Finally, it queries the various metadata associated with each index and verifies the information
        /// is correct.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-286
        /// @expected_result Multiple secondary indexes should be created on the same column
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void MultipleSecondaryIndexTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      features map<text, text>)";
            session.Execute(cql);
            cql = "CREATE INDEX idx_map_keys ON " + tableName + "(KEYS(features))";
            session.Execute(cql);
            cql = "CREATE INDEX idx_map_values ON " + tableName + "(VALUES(features))";
            session.Execute(cql);

            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.AreEqual(2, tableMeta.Indexes.Count);

            var mapKeysIndex = tableMeta.Indexes["idx_map_keys"];
            Assert.AreEqual("idx_map_keys", mapKeysIndex.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, mapKeysIndex.Kind);
            Assert.AreEqual("keys(features)", mapKeysIndex.Target);
            Assert.NotNull(mapKeysIndex.Options);

            var mapValuesIndex = tableMeta.Indexes["idx_map_values"];
            Assert.AreEqual("idx_map_values", mapValuesIndex.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, mapValuesIndex.Kind);
            Assert.AreEqual("values(features)", mapValuesIndex.Target);
            Assert.NotNull(mapValuesIndex.Options);
        }

        /// Tests that multiple secondary indexes are not supported per duplicate column
        /// 
        /// RaiseErrorOnInvalidMultipleSecondaryIndexTest tests that multiple secondary indexes cannot be created on the same duplicate column.
        /// It first creates a table with a simple text column to be used by the secondary index. It then proceeds to create a secondary index 
        /// on this text column, and verifies that the driver metadata is updated. It then attempts to re-create the same secondary index on the
        /// exact same column, and verifies that an exception is raised. It then attempts once again to re-create the same secondary index on the
        /// same column, but this time giving an explicit index name, verifying an exception is raised. Finally, it queries the driver metadata 
        /// and verifies that only one index was actually created.
        /// 
        /// @expected_error RequestInvalidException If a secondary index is re-attempted to be created on the same column
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-286
        /// @expected_result Multiple secondary indexes should not be created on the same column in each case
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void RaiseErrorOnInvalidMultipleSecondaryIndexTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      description text)";
            session.Execute(cql);

            var indexName = tableName + "_description_idx";
            cql = "CREATE INDEX " + indexName + " ON " + tableName + "(description)";
            session.Execute(cql);
            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.AreEqual(1, tableMeta.Indexes.Count);

            Assert.Throws<InvalidQueryException>(() => session.Execute(cql));

            var cql2 = "CREATE INDEX idx2 ON " + tableName + "(description)";
            Assert.Throws<InvalidQueryException>(() => session.Execute(cql2));

            Assert.AreEqual(1, tableMeta.Indexes.Count);
            var descriptionIndex = tableMeta.Indexes[indexName];
            Assert.AreEqual(indexName, descriptionIndex.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, descriptionIndex.Kind);
            Assert.AreEqual("description", descriptionIndex.Target);
            Assert.NotNull(descriptionIndex.Options);
        }

        /// Tests that clustering order metadata is set properly
        /// 
        /// ColumnClusteringOrderReversedTest tests that clustering order metadata for a clustering key is properly recalled in the driver
        /// metadata under the "ClusteringKeys" metadata. It first creates a simple table with a primary key, one column ascending, and another
        /// column descending. It checks the metadata for each clustering key to make sure that the proper value is recalled in the driver metadata.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-359
        /// @expected_result Clustering order metadata is properly set
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void ColumnClusteringOrderReversedTest()
        {
            if (CassandraVersion >= Version.Parse("4.0"))
            {
                Assert.Ignore("Compact table test designed for C* 3.0");
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewCluster();
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int,
                      description text,
                      price double,
                      PRIMARY KEY(id, description, price)
                      ) WITH COMPACT STORAGE
                      AND CLUSTERING ORDER BY (description ASC, price DESC)";
            session.Execute(cql);

            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.AreEqual(new[] { "description", "price" }, tableMeta.ClusteringKeys.Select(c => c.Item1.Name));
            Assert.AreEqual(new[] { SortOrder.Ascending, SortOrder.Descending }, tableMeta.ClusteringKeys.Select(c => c.Item2));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CassandraVersion_Should_Be_Obtained_From_Host_Metadata()
        {
            foreach (var host in Cluster.AllHosts())
            {
                Assert.NotNull(host.CassandraVersion);
                Assert.Greater(host.CassandraVersion, new Version(1, 2));
            }
        }

        [Test, TestCassandraVersion(4, 0)]
        public void Virtual_Table_Metadata_Test()
        {
            var cluster = GetNewCluster();
            var table = cluster.Metadata.GetTable("system_views", "clients");
            Assert.NotNull(table);
            Assert.True(table.IsVirtual);
            Assert.AreEqual(table.PartitionKeys.Select(c => c.Name), new[] { "address" });
            Assert.AreEqual(table.ClusteringKeys.Select(t => t.Item1.Name), new[] { "port" });
        }

        [Test, TestCassandraVersion(4, 0)]
        public void Virtual_Keyspaces_Are_Included()
        {
            var cluster = GetNewCluster();
            var defaultVirtualKeyspaces = new[] {"system_views", "system_virtual_schema"};
            CollectionAssert.IsSubsetOf(defaultVirtualKeyspaces, cluster.Metadata.GetKeyspaces());

            foreach (var keyspaceName in defaultVirtualKeyspaces)
            {
                var ks = cluster.Metadata.GetKeyspace(keyspaceName);
                Assert.True(ks.IsVirtual);
                Assert.AreEqual(keyspaceName, ks.Name);
            }

            // "system" keyspace is still a regular keyspace
            Assert.False(cluster.Metadata.GetKeyspace("system").IsVirtual);
        }
    }
}
