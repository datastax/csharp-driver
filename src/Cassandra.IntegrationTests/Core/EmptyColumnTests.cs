//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    public class EmptyColumnTests : TestGlobals
    {
        [Test]
        [TestCassandraVersion(3, 0, 0)]
        public void Should_ReturnCorrectValue_When_EmptyColumnNameAndSchemaParserV2()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system_schema.tables WHERE table_name='testtable' AND keyspace_name='testks'"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new
                                {
                                    compression = new { },
                                    compaction = new { },
                                    bloom_filter_fp_chance = 0.1,
                                    caching = new { keys = "ALL", rows_per_partition = "NONE" },
                                    comment = "comment",
                                    gc_grace_seconds = 60000,
                                    dclocal_read_repair_chance = 0.1,
                                    read_repair_chance = 0.1,
                                    keyspace_name = "testks"
                                }
                            },
                        column_types = new
                        {
                            compression = "map<ascii, ascii>",
                            compaction = "map<ascii, ascii>",
                            bloom_filter_fp_chance = "double",
                            caching = "map<ascii, ascii>",
                            comment = "ascii",
                            gc_grace_seconds = "int",
                            dclocal_read_repair_chance = "double",
                            read_repair_chance = "double",
                            keyspace_name = "ascii"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system_schema.keyspaces"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new
                                {
                                    replication = new
                                    {
                                        @class = "SimpleStrategy",
                                        replication_factor = "1"
                                    },
                                    keyspace_name = "testks",
                                    durable_writes = true
                                }
                            },
                        column_types = new
                        {
                            replication = "map<ascii, ascii>",
                            keyspace_name = "ascii",
                            durable_writes = "boolean"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system_schema.indexes WHERE table_name='testtable' AND keyspace_name='testks'"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new
                                {
                                    keyspace_name = "ascii",
                                    table_name = "ascii",
                                    index_name = "ascii",
                                    kind = "ascii",
                                    options = new { target = "Custom" }
                                }
                            },
                        column_types = new
                        {
                            keyspace_name = "ascii",
                            table_name = "ascii",
                            index_name = "ascii",
                            kind = "ascii",
                            options = "map<ascii,ascii>"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system_schema.columns WHERE table_name='testtable' AND keyspace_name='testks'"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new
                                {
                                    keyspace_name ="testks",
                                    table_name = "testtable",
                                    column_name = "",
                                    clustering_order = "none",
                                    column_name_bytes = 0x12,
                                    kind = "partition_key",
                                    position = 0,
                                    type = "text"
                                }
                            },
                        column_types = new
                        {
                            keyspace_name = "ascii",
                            table_name = "ascii",
                            column_name = "ascii",
                            clustering_order = "ascii",
                            column_name_bytes = "blob",
                            kind = "ascii",
                            position = "int",
                            type = "ascii"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT \"\", \" \" FROM testks.testtable"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                            new Dictionary<string, string>
                            {
                                {"", "testval"},
                                {" ", "testval2"}
                            }
                        },
                        column_types = new Dictionary<string, string>
                        {
                            {"", "ascii"},
                            {" ", "ascii"}
                        },
                        ignore_on_prepare = false
                    }
                });
                
                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?",
                        @params = new
                        {
                            column1 = "testval",
                            column2 = "testval2"
                        },
                        param_types = new 
                        {
                            column1 = "ascii",
                            column2 = "ascii"
                        }
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new Dictionary<string, string>
                                {
                                    {"", "testval"},
                                    {" ", "testval2"}
                                }
                            },
                        column_types = new Dictionary<string, string>
                            {
                                {"", "ascii"},
                                {" ", "ascii"}
                            },
                        ignore_on_prepare = false
                    }
                });

                var mapConfig = new MappingConfiguration();
                mapConfig.Define(
                    new Map<TestTable>()
                        .KeyspaceName("testks")
                        .TableName("testtable")
                        .PartitionKey(u => u.TestColumn)
                        .Column(u => u.TestColumn, cm => cm.WithName(""))
                        .Column(u => u.TestColumn2, cm => cm.WithName(" ")));

                using (var cluster = EmptyColumnTests.BuildCluster(simulacronCluster))
                {
                    var session = cluster.Connect();

                    var testTables = new Table<TestTable>(session);
                    var test = (from t in testTables.Execute() select t).First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var mapper = new Mapper(session, mapConfig);
                    test = mapper.Fetch<TestTable>().First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var tableMetadata = session.Cluster.Metadata.GetTable("testks", "testtable");
                    Assert.IsNotNull(tableMetadata);

                    var rs = session.Execute("SELECT \"\", \" \" FROM testks.testtable");
                    AssertRowSetContainsCorrectValues(rs);

                    var ps = session.Prepare("SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?");
                    var bs = ps.Bind("testval", "testval2");
                    rs = session.Execute(bs);
                    AssertRowSetContainsCorrectValues(rs);
                }
            }
        }
        
        [Test]
        [TestCassandraVersion(3, 0, 0, Comparison.LessThan)]
        public void Should_ReturnCorrectValue_When_EmptyColumnNameAndSchemaParserV1()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system.schema_keyspaces"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                            new
                            {
                                strategy_options = "{\"replication_factor\":\"1\"}",
                                strategy_class = "SimpleStrategy",
                                keyspace_name = "testks",
                                durable_writes = true
                            }
                        },
                        column_types = new
                        {
                            strategy_options = "ascii",
                            keyspace_name = "ascii",
                            durable_writes = "boolean",
                            strategy_class = "ascii"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='testtable' AND keyspace_name='testks'"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                            new
                            {
                                compression = "{}",
                                compression_parameters = "{}",
                                compaction_strategy_class = "compaction",
                                compaction_strategy_options = "{}",
                                bloom_filter_fp_chance = 0.1,
                                caching = "{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}",
                                comment = "comment",
                                gc_grace_seconds = 60000,
                                dclocal_read_repair_chance = 0.1,
                                read_repair_chance = 0.1,
                                keyspace_name = "testks",
                                local_read_repair_chance = 0.1,
                                comparator = ""
                            }
                        },
                        column_types = new
                        {
                            compression = "ascii",
                            compression_parameters = "ascii",
                            compaction_strategy_class = "ascii",
                            compaction_strategy_options = "ascii",
                            bloom_filter_fp_chance = "double",
                            caching = "ascii",
                            comment = "ascii",
                            gc_grace_seconds = "int",
                            dclocal_read_repair_chance = "double",
                            read_repair_chance = "double",
                            local_read_repair_chance = "double",
                            keyspace_name = "ascii",
                            comparator = "ascii"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT * FROM system.schema_columns WHERE columnfamily_name='testtable' AND keyspace_name='testks'"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new
                                {
                                    keyspace_name ="testks",
                                    columnfamily_name = "testtable",
                                    column_name = "",
                                    clustering_order = "none",
                                    column_name_bytes = 0x12,
                                    kind = "partition_key",
                                    position = 0,
                                    type = "text",
                                    validator = "validator",
                                    index_name = "",
                                    index_type = "",
                                    index_options = "{}"
                                }
                            },
                        column_types = new
                        {
                            keyspace_name = "ascii",
                            columnfamily_name = "ascii",
                            column_name = "ascii",
                            clustering_order = "ascii",
                            column_name_bytes = "blob",
                            kind = "ascii",
                            position = "int",
                            type = "ascii",
                            validator = "ascii",
                            index_name = "ascii",
                            index_type = "ascii",
                            index_options = "ascii"
                        },
                        ignore_on_prepare = false
                    }
                });

                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT \"\", \" \" FROM testks.testtable"
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                                new Dictionary<string, string>
                                {
                                    {"", "testval"},
                                    {" ", "testval2"}
                                }
                            },
                        column_types = new Dictionary<string, string>
                            {
                                {"", "ascii"},
                                {" ", "ascii"}
                            },
                        ignore_on_prepare = false
                    }
                });
                
                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = "SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?",
                        @params = new
                        {
                            column1 = "testval",
                            column2 = "testval2"
                        },
                        param_types = new 
                        {
                            column1 = "ascii",
                            column2 = "ascii"
                        }
                    },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 0,
                        rows = new[]
                        {
                            new Dictionary<string, string>
                            {
                                {"", "testval"},
                                {" ", "testval2"}
                            }
                        },
                        column_types = new Dictionary<string, string>
                        {
                            {"", "ascii"},
                            {" ", "ascii"}
                        },
                        ignore_on_prepare = false
                    }
                });

                var mapConfig = new MappingConfiguration();
                mapConfig.Define(
                    new Map<TestTable>()
                        .KeyspaceName("testks")
                        .TableName("testtable")
                        .PartitionKey(u => u.TestColumn)
                        .Column(u => u.TestColumn, cm => cm.WithName(""))
                        .Column(u => u.TestColumn2, cm => cm.WithName(" ")));

                using (var cluster = EmptyColumnTests.BuildCluster(simulacronCluster))
                {
                    var session = cluster.Connect();

                    var testTables = new Table<TestTable>(session);
                    var test = (from t in testTables.Execute() select t).First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var mapper = new Mapper(session, mapConfig);
                    test = mapper.Fetch<TestTable>().First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var tableMetadata = session.Cluster.Metadata.GetTable("testks", "testtable");
                    Assert.IsNotNull(tableMetadata);

                    var rs = session.Execute("SELECT \"\", \" \" FROM testks.testtable");
                    AssertRowSetContainsCorrectValues(rs);

                    var ps = session.Prepare("SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?");
                    var bs = ps.Bind("testval", "testval2");
                    rs = session.Execute(bs);
                    AssertRowSetContainsCorrectValues(rs);
                }
            }
        }

        private void AssertRowSetContainsCorrectValues(RowSet rs)
        {
            var row = rs.Single();
            Assert.IsTrue(rs.Columns.Length == 2 && rs.Columns.Any(c => c.Name == string.Empty) && rs.Columns.Any(c => c.Name == " "));
            Assert.AreEqual("testval", row.GetValue<string>(string.Empty));
            Assert.AreEqual("testval2", row.GetValue<string>(" "));
        }

        private static Cluster BuildCluster(SimulacronCluster simulacronCluster)
        {
            return Cluster.Builder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .Build();
        }

        [Cassandra.Mapping.Attributes.Table(Name = "testtable", Keyspace = "testks")]
        private class TestTable
        {
            [Cassandra.Mapping.Attributes.Column("")]
            public string TestColumn { get; set; }

            [Cassandra.Mapping.Attributes.Column(" ")]
            public string TestColumn2 { get; set; }
        }
    }
}