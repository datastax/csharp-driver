using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.Tests
{
    [TestFixture]
    public class SchemaParserTests
    {
        private static SchemaParserV1 GetV1Instance(IMetadataQueryProvider cc)
        {
            var metadata = new Metadata(new Configuration())
            {
                ControlConnection = cc
            };
            metadata.SetCassandraVersion(new Version(2, 0));
            return new SchemaParserV1(metadata);
        }

        private static SchemaParserV2 GetV2Instance(IMetadataQueryProvider cc, Func<string, string, Task<UdtColumnInfo>> udtResolver = null)
        {
            var metadata = new Metadata(new Configuration())
            {
                ControlConnection = cc
            };
            metadata.SetCassandraVersion(new Version(3, 0));
            return new SchemaParserV2(metadata, udtResolver);
        }
        
        /// <summary>
        /// Helper method to get a QueryTrace instance..
        /// </summary>
        private static QueryTrace GetQueryTrace()
        {
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            return new QueryTrace(Guid.NewGuid(), sessionMock.Object);
        }

        [Test]
        public void SchemaParserV1_GetKeyspace_Should_Return_Null_When_Not_Found()
        {
            const string ksName = "ks1";
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_keyspaces.*" + ksName), It.IsAny<bool>()))
                .Returns(() => TaskHelper.ToTask(Enumerable.Empty<Row>()));
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(ksName));
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
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(ksName));
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
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","id"   },{"component_index",null},{"index_name",null},{"index_options",null},{"index_type",null},{"type","partition_key"},{"validator","org.apache.cassandra.db.marshal.UUIDType"}},
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","text1"},{"component_index",null},{"index_name",null},{"index_options",null},{"index_type",null},{"type","regular"      },{"validator","org.apache.cassandra.db.marshal.UTF8Type"}},
              new Dictionary<string, object>{{"keyspace_name","ks_tbl_meta"},{"columnfamily_name","tbl1"},{"column_name","text2"},{"component_index",null},{"index_name",null},{"index_options",null},{"index_type",null},{"type","regular"      },{"validator","org.apache.cassandra.db.marshal.UTF8Type"}}
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
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
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
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            var table = ks.GetTableMetadata("ks_tbl_meta");
            Assert.False(table.Options.IsCompactStorage);
            Assert.NotNull(table.Options.Caching);
            Assert.AreEqual(5, table.TableColumns.Length);
            CollectionAssert.AreEqual(new[] { "pk1", "apk2" }, table.PartitionKeys.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "zck" }, table.ClusteringKeys.Select(c => c.Item1.Name));
            CollectionAssert.AreEqual(new[] { SortOrder.Ascending }, table.ClusteringKeys.Select(c => c.Item2));
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
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
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
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            var ex = Assert.Throws<ArgumentException>(() => ks.GetTableMetadata("ks_tbl_meta"));
            StringAssert.Contains("column_name", ex.Message);
        }

        [Test]
        public void SchemaParserV1_GetTable_Should_Propagate_Exceptions_From_Query_Provider()
        {
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
                .Returns(() => TaskHelper.FromException<IEnumerable<Row>>(new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>())));
            //This will cause the task to be faulted
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system\\.schema_columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.FromException<IEnumerable<Row>>(new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>())));
            var parser = GetV1Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            Assert.Throws<NoHostAvailableException>(() => ks.GetTableMetadata("tbl1"));
        }

        [Test]
        public void SchemaParserV2_GetKeyspace_Should_Retrieve_And_Parse_Keyspace_With_SimpleStrategy()
        {
            const string ksName = "ks1";
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", ksName},
                {"durable_writes", true},
                {"replication", new Dictionary<string, string>{{"class", "org.apache.cassandra.locator.SimpleStrategy"}, {"replication_factor", "2"}}},
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.keyspaces.*" + ksName), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { row }));
            var parser = GetV2Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace(ksName));
            Assert.NotNull(ks);
            Assert.AreEqual(ksName, ks.Name);
            Assert.AreEqual(true, ks.DurableWrites);
            Assert.AreEqual("SimpleStrategy", ks.StrategyClass);
            CollectionAssert.AreEqual(new Dictionary<string, int> { { "replication_factor", 2 } }, ks.Replication);
        }

        [Test]
        public void SchemaParserV2_GetTable_Should_Parse_3_0_Table_With_Partition_And_Clustering_Keys()
        {
            var tableRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name","ks_tbl_meta"},
                {"table_name","tbl4"},
                {"bloom_filter_fp_chance",0.01},
                {"caching",new SortedDictionary<string, string>{{"keys","ALL"},{"rows_per_partition","NONE"}}},
                {"comment",""},
                {"compaction",new SortedDictionary<string, string>{{"min_threshold","4"},{"max_threshold","32"},{"class","org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"}}},
                {"compression",new SortedDictionary<string, string>{{"chunk_length_in_kb","64"},{"class","org.apache.cassandra.io.compress.LZ4Compressor"}}},
                {"dclocal_read_repair_chance",0.1},
                {"default_time_to_live",0},
                {"extensions",new Dictionary<string, string>()},
                {"flags", new []{"compound"}},
                {"gc_grace_seconds",864000},
                {"id","8008ae40-5862-11e5-b0ce-c7d0c38d1d8d"},
                {"max_index_interval",2048},
                {"memtable_flush_period_in_ms",0},{"min_index_interval",128},{"read_repair_chance",0D},{"speculative_retry","99PERCENTILE"}
            });
            var columnRows = new[]
            {
                new Dictionary<string, object>{{"keyspace_name", "ks_tbl_meta"}, {"table_name", "tbl4"}, {"column_name", "apk2" }, {"clustering_order", "none"}, {"column_name_bytes", "0x61706b32"  }, {"kind", "partition_key"}, {"position",  1 }, {"type", "text"}},
                new Dictionary<string, object>{{"keyspace_name", "ks_tbl_meta"}, {"table_name", "tbl4"}, {"column_name", "pk1"  }, {"clustering_order", "none"}, {"column_name_bytes", "0x706b31"    }, {"kind", "partition_key"}, {"position",  0 }, {"type", "uuid"}},
                new Dictionary<string, object>{{"keyspace_name", "ks_tbl_meta"}, {"table_name", "tbl4"}, {"column_name", "val2" }, {"clustering_order", "none"}, {"column_name_bytes", "0x76616c32"  }, {"kind", "regular"      }, {"position",  -1}, {"type", "blob"}},
                new Dictionary<string, object>{{"keyspace_name", "ks_tbl_meta"}, {"table_name", "tbl4"}, {"column_name", "valz1"}, {"clustering_order", "none"}, {"column_name_bytes", "0x76616c7a31"}, {"kind", "regular"      }, {"position",  -1}, {"type", "int"}},
                new Dictionary<string, object>{{"keyspace_name", "ks_tbl_meta"}, {"table_name", "tbl4"}, {"column_name", "zck"  }, {"clustering_order", "asc" }, {"column_name_bytes", "0x7a636b"    }, {"kind", "clustering"   }, {"position",  0 }, {"type", "timeuuid"}}
            }.Select(TestHelper.CreateRow);

            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"}, {"durable_writes", true}, {"replication", new SortedDictionary<string, string>{{"class", "org.apache.cassandra.locator.SimpleStrategy"}, {"replication_factor", "1"}}},
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.tables.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(columnRows));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.indexes.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(Enumerable.Empty<Row>()));
            var parser = GetV2Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            var table = ks.GetTableMetadata("ks_tbl_meta");
            Assert.False(table.Options.IsCompactStorage);
            Assert.AreEqual("{\"keys\":\"ALL\",\"rows_per_partition\":\"NONE\"}", table.Options.Caching);
            CollectionAssert.AreEquivalent(new[] { "pk1", "apk2", "val2", "valz1", "zck" }, table.TableColumns.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "pk1", "apk2" }, table.PartitionKeys.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "zck" }, table.ClusteringKeys.Select(c => c.Item1.Name));
            CollectionAssert.AreEqual(new[] { SortOrder.Ascending }, table.ClusteringKeys.Select(c => c.Item2));
        }

        [Test]
        public void SchemaParserV2_GetTable_Should_Parse_3_0_Table_With_SecondaryIndexes()
        {
            var tableRow = TestHelper.CreateRow(new SortedDictionary<string, object>
            {
                {"keyspace_name","ks_tbl_meta"},
                {"table_name","tbl4"},
                {"bloom_filter_fp_chance",0.01},
                {"caching",new SortedDictionary<string, string>{{"keys","ALL"},{"rows_per_partition","NONE"}}},
                {"comment",""},
                {"compaction",new SortedDictionary<string, string>{{"min_threshold","4"},{"max_threshold","32"},{"class","org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"}}},
                {"compression",new SortedDictionary<string, string>{{"chunk_length_in_kb","64"},{"class","org.apache.cassandra.io.compress.LZ4Compressor"}}},
                {"dclocal_read_repair_chance",0.1},
                {"default_time_to_live",0},
                {"extensions",new SortedDictionary<string, string>()},
                {"flags", new []{"compound"}},
                {"gc_grace_seconds",864000},
                {"id","8008ae40-5862-11e5-b0ce-c7d0c38d1d8d"},
                {"max_index_interval",2048},
                {"memtable_flush_period_in_ms",0},{"min_index_interval",128},{"read_repair_chance",0D},{"speculative_retry","99PERCENTILE"}
            });
            var columnRows = new[]
            {
                new Dictionary<string, object>{{"keyspace_name", "ks1"}, {"table_name", "tbl4"}, {"column_name", "pk"  }, {"clustering_order", "none"}, {"column_name_bytes", "0x706b31"    }, {"kind", "partition_key"}, {"position",  0 }, {"type", "uuid"}},
                new Dictionary<string, object>{{"keyspace_name", "ks1"}, {"table_name", "tbl4"}, {"column_name", "ck"  }, {"clustering_order", "desc" }, {"column_name_bytes", "0x7a636b"    }, {"kind", "clustering"   }, {"position",  0 }, {"type", "timeuuid"}},
                new Dictionary<string, object>{{"keyspace_name", "ks1"}, {"table_name", "tbl4"}, {"column_name", "val" }, {"clustering_order", "none"}, {"column_name_bytes", "0x76616c32"  }, {"kind", "regular"      }, {"position",  -1}, {"type", "blob"}}
            }.Select(TestHelper.CreateRow);
            var indexRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"},
                {"table_name", "tbl4"},
                {"index_name", "ix1"},
                {"kind", "COMPOSITES"},
                {"options", new Dictionary<string, string>
                {
                    {"target", "val"}
                }}
            });
            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"}, {"durable_writes", true}, {"replication", new Dictionary<string, string>{{"class", "org.apache.cassandra.locator.SimpleStrategy"}, {"replication_factor", "1"}}},
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.tables.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { tableRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(columnRows));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.indexes.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] {indexRow}));
            var parser = GetV2Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            var table = ks.GetTableMetadata("tbl4");
            Assert.False(table.Options.IsCompactStorage);
            CollectionAssert.AreEquivalent(new[] { "pk", "ck", "val"}, table.TableColumns.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "pk" }, table.PartitionKeys.Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "ck" }, table.ClusteringKeys.Select(c => c.Item1.Name));
            CollectionAssert.AreEqual(new[] { SortOrder.Descending }, table.ClusteringKeys.Select(c => c.Item2));
            Assert.NotNull(table.Indexes);
            Assert.AreEqual(1, table.Indexes.Count);
            var index = table.Indexes["ix1"];
            Assert.AreEqual("ix1", index.Name);
            Assert.AreEqual(IndexMetadata.IndexKind.Composites, index.Kind);
            Assert.AreEqual("val", index.Target);
            Assert.NotNull(index.Options);
        }

        [Test]
        public void SchemaParserV2_GetTable_Should_Propagate_Exceptions_From_Query_Provider()
        {
            var ksRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"keyspace_name", "ks1"}, {"durable_writes", true}, {"replication", new Dictionary<string, string>{{"class", "org.apache.cassandra.locator.SimpleStrategy"}, {"replication_factor", "1"}}},
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.keyspaces.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { ksRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.tables.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(Enumerable.Empty<Row>()));
            //This will cause the task to be faulted
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.columns.*ks1"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.FromException<IEnumerable<Row>>(new NoHostAvailableException(new Dictionary<IPEndPoint,Exception>())));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_schema\\.indexes.*ks1"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(Enumerable.Empty<Row>()));
            var parser = GetV2Instance(queryProviderMock.Object);
            var ks = TaskHelper.WaitToComplete(parser.GetKeyspace("ks1"));
            Assert.NotNull(ks);
            Assert.Throws<NoHostAvailableException>(() => ks.GetTableMetadata("tbl1"));
        }

        [Test]
        public void SchemaParser_GetQueryTrace_Should_Query_Traces_Tables()
        {
            var sessionRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"duration", 10},
                {"request", "test query"},
                {"coordinator", IPAddress.Parse("192.168.12.13")},
                {"parameters", null},
                {"started_at", DateTimeOffset.Now}
            });
            var eventRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"activity", "whatever"},
                {"event_id", TimeUuid.NewId()},
                {"source_elapsed", 100 },
                {"source", IPAddress.Parse("192.168.1.100")},
                {"thread", "thread name"}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.sessions"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { sessionRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.events"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { eventRow }));

            var queryTrace = GetQueryTrace();
            var parser = GetV1Instance(queryProviderMock.Object);
            var timer = new HashedWheelTimer();
            var resultTrace = TaskHelper.WaitToComplete(parser.GetQueryTrace(queryTrace, timer));
            Assert.AreSame(queryTrace, resultTrace);
            Assert.Greater(queryTrace.DurationMicros, 0);
            Assert.AreEqual("test query", queryTrace.RequestType);
            Assert.AreEqual(1, queryTrace.Events.Count);
            Assert.AreEqual("whatever", queryTrace.Events.First().Description);
            timer.Dispose();
        }

        [Test]
        public void SchemaParser_GetQueryTrace_When_First_QueryAsync_Fails_Exception_Should_Propagate()
        {
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.sessions"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.FromException<IEnumerable<Row>>(new Exception("Test exception")));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.events"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask(Enumerable.Empty<Row>()));

            var queryTrace = GetQueryTrace();
            var parser = GetV1Instance(queryProviderMock.Object);
            var timer = new HashedWheelTimer();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(parser.GetQueryTrace(queryTrace, timer)));
            Assert.AreEqual("Test exception", ex.Message);
            timer.Dispose();
        }

        [Test]
        public void SchemaParser_GetQueryTrace_When_Second_QueryAsync_Fails_Exception_Should_Propagate()
        {
            var sessionRow = TestHelper.CreateRow(new Dictionary<string, object>
            {
                {"duration", 10},
                {"request", "test query"},
                {"coordinator", IPAddress.Parse("192.168.12.13")},
                {"parameters", null},
                {"started_at", DateTimeOffset.Now}
            });
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.sessions"), It.IsAny<bool>()))
                .Returns(() => TestHelper.DelayedTask<IEnumerable<Row>>(new[] { sessionRow }));
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.events"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.FromException<IEnumerable<Row>>(new Exception("Test exception 2")));

            var queryTrace = GetQueryTrace();
            var parser = GetV1Instance(queryProviderMock.Object);
            var timer = new HashedWheelTimer();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(parser.GetQueryTrace(queryTrace, timer)));
            Assert.AreEqual("Test exception 2", ex.Message);
            timer.Dispose();
        }

        [Test]
        public void SchemaParser_GetQueryTrace_Should_Try_Multiple_Times_To_Get_The_Trace()
        {
            var counter = 0;
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.sessions"), It.IsAny<bool>()))
                .Returns(() =>
                {
                    var sessionRow = TestHelper.CreateRow(new Dictionary<string, object>
                    {
                        {"duration", ++counter >= 3 ? counter : (int?)null },
                        {"request", "test query " + counter},
                        {"coordinator", IPAddress.Parse("192.168.12.13")},
                        {"parameters", null},
                        {"started_at", DateTimeOffset.Now}
                    });
                    return TestHelper.DelayedTask<IEnumerable<Row>>(new[] {sessionRow});
                });
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.events"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.ToTask(Enumerable.Empty<Row>()));
            var queryTrace = GetQueryTrace();
            var parser = GetV1Instance(queryProviderMock.Object);
            var timer = new HashedWheelTimer();
            TaskHelper.WaitToComplete(parser.GetQueryTrace(queryTrace, timer));
            Assert.AreEqual(counter, 3);
            Assert.AreEqual("test query 3", queryTrace.RequestType);
            timer.Dispose();
        }

        [Test]
        public void SchemaParser_GetQueryTrace_Should_Not_Try_More_Than_Max_Attempts()
        {
            var counter = 0;
            var queryProviderMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.sessions"), It.IsAny<bool>()))
                .Returns(() =>
                {
                    counter++;
                    var sessionRow = TestHelper.CreateRow(new Dictionary<string, object>
                    {
                        {"duration", null },
                        {"request", "test query "},
                        {"coordinator", IPAddress.Parse("192.168.12.13")},
                        {"parameters", null},
                        {"started_at", DateTimeOffset.Now}
                    });
                    return TestHelper.DelayedTask<IEnumerable<Row>>(new[] { sessionRow });
                });
            queryProviderMock
                .Setup(cc => cc.QueryAsync(It.IsRegex("system_traces\\.events"), It.IsAny<bool>()))
                .Returns(() => TaskHelper.ToTask(Enumerable.Empty<Row>()));
            var queryTrace = GetQueryTrace();
            var parser = GetV1Instance(queryProviderMock.Object);
            var timer = new HashedWheelTimer();
            Assert.Throws<TraceRetrievalException>(() => TaskHelper.WaitToComplete(parser.GetQueryTrace(queryTrace, timer)));
            Assert.AreEqual(counter, 5);
            timer.Dispose();
        }
    }
}