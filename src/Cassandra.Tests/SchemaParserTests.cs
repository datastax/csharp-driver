using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class SchemaParserTests
    {
        [Test]
        public void SchemaParserV1_GetKeyspace_Should_Return_Null_When_Not_Found()
        {
            const string ksName = "ks1";
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*" + ksName), It.IsAny<bool>()))
                .Returns(() => TaskHelper.ToTask(Enumerable.Empty<Row>()));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, ksName));
            Assert.Null(ks);
        }

        [Test]
        public void SchemaParserV1_GetKeyspace_Should_Retrieve_And_Parse_Keyspace_With_SimpleStrategy()
        {
            const string ksName = "ks1";
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", ksName},
                {"durable_writes", true},
                {"strategy_class", "Simple"},
                {"strategy_options", "{\"replication_factor\": \"4\"}"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*" + ksName), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { row }));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, ksName));
            Assert.NotNull(ks);
            Assert.AreEqual(ksName, ks.Name);
            Assert.AreEqual(true, ks.DurableWrites);
            Assert.AreEqual("Simple", ks.StrategyClass);
            CollectionAssert.AreEqual(new Dictionary<string, int> {{"replication_factor", 4}}, ks.Replication);
        }

        [Test]
        public void SchemaParserV1_GetTable_Should_Parse_2_0_Table_With_Compact_Storage()
        {
            var tableRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks_tbl_meta"},
                {"columnfamily_name","tbl1"},
                {"bloom_filter_fp_chance", 0.01},
                {"caching", "{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}"},
                {"cf_id","609f53a0-038b-11e5-be48-0d419bfb85c8"},
                {"column_aliases","[]"},
                {"comment",""},
                {"compaction_strategy_class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},
                {"compaction_strategy_options", "{}"},
                {"comparator","org.apache.cassandra.db.marshal.UTF8Type"},
                {"compression_parameters","{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}"},
                {"default_time_to_live",0},
                {"default_validator","org.apache.cassandra.db.marshal.BytesType"},
                {"dropped_columns",null},
                {"gc_grace_seconds",864000},
                {"index_interval",null},
                {"is_dense",false},
                {"key_aliases","[\"id\"]"},
                {"key_validator","org.apache.cassandra.db.marshal.UUIDType"},
                {"local_read_repair_chance",0.1},
                {"max_compaction_threshold",32},
                {"max_index_interval",2048},
                {"memtable_flush_period_in_ms",0},
                {"min_compaction_threshold",4},
                {"min_index_interval",128},
                {"read_repair_chance",0D},
                {"speculative_retry","99.0PERCENTILE"},
                {"subcomparator",null},
                {"type","Standard"},
                {"value_alias",null}
            });
            var columnRows = new [] {
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","id"   },{"component_index",null},{"index_name",null},{"index_options","null"},{"index_type",null},{"type","partition_key"},{"validator","org.apache.cassandra.db.marshal.UUIDType"}},
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","text1"},{"component_index",null},{"index_name",null},{"index_options","null"},{"index_type",null},{"type","regular"      },{"validator","org.apache.cassandra.db.marshal.UTF8Type"}},
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","text2"},{"component_index",null},{"index_name",null},{"index_options","null"},{"index_type",null},{"type","regular"      },{"validator","org.apache.cassandra.db.marshal.UTF8Type"}}
            }.Select(TestHelper.CreateRow);

            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"},
                {"durable_writes", true},
                {"strategy_class", "Simple"},
                {"strategy_options", "{\"replication_factor\": \"1\"}"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columnfamilies.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(columnRows));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, "ks1"));
            Assert.NotNull(ks);
            var table = ks.GetTableMetadata("ks_tbl_meta");
            Assert.True(table.Options.IsCompactStorage);
            Assert.NotNull(table.Options.Caching);
            Assert.AreEqual(3, table.TableColumns.Length);
            CollectionAssert.AreEqual(table.TableColumns.Select(c => c.Name), new[] { "id", "text1", "text2" });
            Assert.AreEqual(ColumnTypeCode.Uuid, table.TableColumns[0].TypeCode);
            CollectionAssert.AreEqual(table.PartitionKeys.Select(c => c.Name), new[] { "id" });
            Assert.AreEqual(0, table.ClusteringKeys.Length);
        }

        [Test]
        public void SchemaParserV1_GetTable_Should_Parse_1_2_Table_With_Partition_And_Clustering_Keys()
        {
            var tableRow = TestHelper.CreateRow(new Dictionary<string, object> 
            { 
                {"keyspace_name", "ks_tbl_meta"}, {"columnfamily_name", "tbl1"}, {"bloom_filter_fp_chance", 0.01}, {"caching", "KEYS_ONLY"}, 
                {"column_aliases", "[\"zck\"]"}, {"comment", ""}, {"compaction_strategy_class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"}, {"compaction_strategy_options", "{}"},
                {"comparator", "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)"}, {"compression_parameters", "{\"sstable_compression\":\"org.apache.cassandra.io.compress.SnappyCompressor\"}"}, {"default_validator", "org.apache.cassandra.db.marshal.BytesType"}, {"gc_grace_seconds", 864000}, {"id", null}, {"key_alias", null},
                {"key_aliases", "[\"pk1\",\"apk2\"]"}, {"key_validator", "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)"}, {"local_read_repair_chance", 0D}, {"max_compaction_threshold", 32}, {"min_compaction_threshold", 4}, {"populate_io_cache_on_flush", false}, {"read_repair_chance", 0.1}, {"replicate_on_write", true}, {"subcomparator", null}, {"type", "Standard"}, {"value_alias", null}
            });
            var columnRows = new []
            {
              new Dictionary<string, object> { {"keyspace_name", "ks_tbl_meta"}, {"columnfamily_name", "tbl1"}, {"column_name", "val2" }, {"component_index", 1}, {"index_name", null}, {"index_options", null}, {"index_type", null}, {"validator", "org.apache.cassandra.db.marshal.BytesType"}},
              new Dictionary<string, object> { {"keyspace_name", "ks_tbl_meta"}, {"columnfamily_name", "tbl1"}, {"column_name", "valz1"}, {"component_index", 1}, {"index_name", null}, {"index_options", null}, {"index_type", null}, {"validator", "org.apache.cassandra.db.marshal.Int32Type"}}
            }.Select(TestHelper.CreateRow);

            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"},
                {"durable_writes", true},
                {"strategy_class", "Simple"},
                {"strategy_options", "{\"replication_factor\": \"1\"}"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columnfamilies.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(columnRows));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, "ks1"));
            Assert.NotNull(ks);
            var table = ks.GetTableMetadata("ks_tbl_meta");
            Assert.False(table.Options.IsCompactStorage);
            Assert.NotNull(table.Options.Caching);
            Assert.AreEqual(5, table.TableColumns.Length);
            CollectionAssert.AreEqual(new[] { "pk1", "apk2" }, table.PartitionKeys.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "zck" }, table.ClusteringKeys.Select(c => c.Name));
        }

        [Test]
        public void SchemaParserV1_GetTable_Should_Throw_ArgumentException_When_A_Table_Column_Not_Defined()
        {
            var tableRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks_tbl_meta"},
                {"columnfamily_name","tbl1"},
                //{"bloom_filter_fp_chance", 0.01}, ---> not defined
                {"caching", "{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}"},
                {"cf_id","609f53a0-038b-11e5-be48-0d419bfb85c8"},
                {"column_aliases","[]"},
                {"comment",""},
                {"compaction_strategy_class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},
                {"compaction_strategy_options", "{}"},
                {"comparator","org.apache.cassandra.db.marshal.UTF8Type"},
                {"compression_parameters","{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}"},
                {"default_time_to_live",0},
                {"default_validator","org.apache.cassandra.db.marshal.BytesType"},
                {"dropped_columns",null},
                {"gc_grace_seconds",864000},
                {"index_interval",null},
                {"is_dense",false},
                {"key_aliases","[\"id\"]"},
                {"key_validator","org.apache.cassandra.db.marshal.UUIDType"},
                {"local_read_repair_chance",0.1},
                {"max_compaction_threshold",32},
                {"max_index_interval",2048},
                {"memtable_flush_period_in_ms",0},
                {"min_compaction_threshold",4},
                {"min_index_interval",128},
                {"read_repair_chance",0D},
                {"speculative_retry","99.0PERCENTILE"},
                {"subcomparator",null},
                {"type","Standard"},
                {"value_alias",null}
            });

            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"}, {"durable_writes", true}, {"strategy_class", "Simple"}, {"strategy_options", "{\"replication_factor\": \"1\"}"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columnfamilies.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(Enumerable.Empty<Row>));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, "ks1"));
            Assert.NotNull(ks);
            var ex = Assert.Throws<ArgumentException>(() => ks.GetTableMetadata("ks_tbl_meta"));
            StringAssert.Contains("bloom_filter_fp_chance", ex.Message);
        }

        [Test]
        public void SchemaParserV1_GetTable_Should_Throw_ArgumentException_When_A_SchemaColumn_Column_Not_Defined()
        {
            var tableRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks_tbl_meta"},
                {"columnfamily_name","tbl1"},
                {"bloom_filter_fp_chance", 0.01},
                {"caching", "{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}"},
                {"cf_id","609f53a0-038b-11e5-be48-0d419bfb85c8"},
                {"column_aliases","[]"},
                {"comment",""},
                {"compaction_strategy_class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},
                {"compaction_strategy_options", "{}"},
                {"comparator","org.apache.cassandra.db.marshal.UTF8Type"},
                {"compression_parameters","{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}"},
                {"default_time_to_live",0},
                {"default_validator","org.apache.cassandra.db.marshal.BytesType"},
                {"dropped_columns",null},
                {"gc_grace_seconds",864000},
                {"index_interval",null},
                {"is_dense",false},
                {"key_aliases","[\"id\"]"},
                {"key_validator","org.apache.cassandra.db.marshal.UUIDType"},
                {"local_read_repair_chance",0.1},
                {"max_compaction_threshold",32},
                {"max_index_interval",2048},
                {"memtable_flush_period_in_ms",0},
                {"min_compaction_threshold",4},
                {"min_index_interval",128},
                {"read_repair_chance",0D},
                {"speculative_retry","99.0PERCENTILE"},
                {"subcomparator",null},
                {"type","Standard"},
                {"value_alias",null}
            });
            var columnRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks_tbl_meta"},
                {"columnfamily_name", "tbl1"},
                //{"column_name", "id"},  --> not defined
                {"component_index", null},
                {"index_name", null},
                {"index_options", "null"},
                {"index_type", null},
                {"type", "partition_key"},
                {"validator", "org.apache.cassandra.db.marshal.UUIDType"}
            });
            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"}, {"durable_writes", true}, {"strategy_class", "Simple"}, {"strategy_options", "{\"replication_factor\": \"1\"}"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columnfamilies.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { columnRow }));
            var parser = SchemaParserV1.Instance;
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(queryProviderMock.Object, "ks1"));
            Assert.NotNull(ks);
            var ex = Assert.Throws<ArgumentException>(() => ks.GetTableMetadata("ks_tbl_meta"));
            StringAssert.Contains("column_name", ex.Message);
        }
    }
}
