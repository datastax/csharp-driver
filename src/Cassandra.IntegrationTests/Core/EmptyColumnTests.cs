//
//       Copyright (C) 2019 DataStax Inc.
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
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    public class EmptyColumnTests
    {
        [Test]
        public void Should_ReturnCorrectValue_When_EmptyColumnName()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(new
                {
                    when = new
                    {
                        query = string.Format(
                            "SELECT * FROM system_schema.tables WHERE table_name='{0}' AND keyspace_name='{1}'",
                            "testtable",
                            "testks")
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
                                    replication = "{'strategy': 'SimpleStrategy', 'replication_factor':'1'}",
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
                        query = string.Format(
                            "SELECT * FROM system_schema.indexes WHERE table_name='{0}' AND keyspace_name='{1}'",
                            "testtable",
                            "testks")
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
                        query = string.Format(
                            "SELECT * FROM system_schema.columns WHERE table_name='{0}' AND keyspace_name='{1}'",
                            "testtable",
                            "testks")
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
                    try
                    {

                        var session = cluster.Connect();

                        var testTables = new Table<TestTable>(session);
                        var test = (from t in testTables.Execute() select t).FirstOrDefault();
                        Assert.AreEqual("testval", test.TestColumn);
                        Assert.AreEqual("testval2", test.TestColumn2);

                        var mapper = new Mapper(session, mapConfig);
                        test = mapper.Fetch<TestTable>().FirstOrDefault();
                        Assert.AreEqual("testval", test.TestColumn);
                        Assert.AreEqual("testval2", test.TestColumn2);

                        var tableMetadata = session.Cluster.Metadata.GetTable("testks", "testtable");
                        Assert.IsNotNull(tableMetadata);

                        var rs = session.Execute("SELECT \"\", \" \" FROM testks.testtable");
                        var row = rs.SingleOrDefault();
                        Assert.IsTrue(rs.Columns.Length == 2 && rs.Columns.Any(c => c.Name == string.Empty) && rs.Columns.Any(c => c.Name == " "));
                        Assert.AreEqual("testval", row?.GetValue<string>(string.Empty));
                        Assert.AreEqual("testval2", row?.GetValue<string>(" "));
                    }
                    finally
                    {
                        var a = "";
                        a.ToString();
                    }
                }
            }
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