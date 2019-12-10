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

using System.Linq;
using System.Text;

using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

namespace Cassandra.IntegrationTests.SimulacronAPI.SystemTables
{
    public static class SystemTablePrimeHelper
    {
        private static string ByteArrayToString(byte[] ba)
        {
            return "0x" + ba.Aggregate(string.Empty, (acc, b) => $"{acc}{b:x2}");
        }

        public static void PrimeSystemSchemaTablesV2(this SimulacronBase simulacronCluster, string keyspace, string table, (string name, string kind, string type)[] columns)
        {
            simulacronCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT * FROM system_schema.tables WHERE table_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("compression", DataType.Map(DataType.Ascii, DataType.Ascii)),
                              ("compaction", DataType.Map(DataType.Ascii, DataType.Ascii)),
                              ("bloom_filter_fp_chance", DataType.Double),
                              ("caching", DataType.Map(DataType.Ascii, DataType.Ascii)),
                              ("comment", DataType.Ascii),
                              ("gc_grace_seconds", DataType.Int),
                              ("dclocal_read_repair_chance", DataType.Double),
                              ("read_repair_chance", DataType.Double),
                              ("keyspace_name", DataType.Ascii)
                          },
                          rows =>
                              rows.WithRow(
                                  new { }, new { }, 0.1, new { keys = "ALL", rows_per_partition = "NONE" },
                                  "comment", 60000, 0.1, 0.1, keyspace)));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM system_schema.keyspaces")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("replication", DataType.Map(DataType.Ascii, DataType.Ascii)),
                              ("keyspace_name", DataType.Ascii),
                              ("durable_writes", DataType.Boolean)
                          },
                          rows => rows.WithRow(new { @class = "SimpleStrategy", replication_factor = "1" }, keyspace, true)));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system_schema.indexes WHERE table_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("keyspace_name", DataType.Ascii),
                              ("table_name", DataType.Ascii),
                              ("index_name", DataType.Ascii),
                              ("kind", DataType.Ascii),
                              ("options", DataType.Map(DataType.Ascii, DataType.Ascii))
                          }));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system_schema.columns WHERE table_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(new[]
                          {
                              ("keyspace_name", DataType.Ascii),
                              ("table_name", DataType.Ascii),
                              ("column_name", DataType.Ascii),
                              ("clustering_order", DataType.Ascii),
                              ("column_name_bytes", DataType.Blob),
                              ("kind", DataType.Ascii),
                              ("position", DataType.Int),
                              ("type", DataType.Ascii)
                          },
                          rows => rows.WithRows(
                              columns
                                  .Select(col =>
                                      new object[]
                                      {
                                          keyspace, table, col.name, "none", SystemTablePrimeHelper.ByteArrayToString(Encoding.UTF8.GetBytes(col.name)), col.kind, 0, col.type
                                      })
                                  .ToArray())));
        }

        public static void PrimeSystemSchemaTablesV1(this SimulacronBase simulacronCluster, string keyspace, string table, (string name, string kind, string type)[] columns)
        {
            simulacronCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("compression", DataType.Ascii),
                              ("compression_parameters", DataType.Ascii),
                              ("compaction_strategy_class",  DataType.Ascii),
                              ("compaction_strategy_options", DataType.Ascii),
                              ("bloom_filter_fp_chance", DataType.Double),
                              ("caching", DataType.Ascii),
                              ("comment", DataType.Ascii),
                              ("gc_grace_seconds", DataType.Int),
                              ("dclocal_read_repair_chance", DataType.Double),
                              ("read_repair_chance", DataType.Double),
                              ("keyspace_name", DataType.Ascii),
                              ("local_read_repair_chance", DataType.Double),
                              ("comparator", DataType.Ascii)
                          },
                          rows =>
                              rows.WithRow(
                                  "{}", "{}", "compaction", "{}", 0.1, "{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}",
                                  "comment", 60000, 0.1, 0.1, keyspace, 0.1, "")));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM system.schema_keyspaces")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("strategy_options", DataType.Ascii),
                              ("strategy_class", DataType.Ascii),
                              ("keyspace_name", DataType.Ascii),
                              ("durable_writes", DataType.Boolean)
                          },
                          rows => rows.WithRow("{\"replication_factor\":\"1\"}", "SimpleStrategy", keyspace, true)));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system.schema_columns WHERE columnfamily_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(new[]
                          {
                              ("keyspace_name", DataType.Ascii),
                              ("columnfamily_name", DataType.Ascii),
                              ("column_name", DataType.Ascii),
                              ("clustering_order", DataType.Ascii),
                              ("column_name_bytes", DataType.Blob),
                              ("kind", DataType.Ascii),
                              ("position", DataType.Int),
                              ("type", DataType.Ascii),
                              ("validator", DataType.Ascii),
                              ("index_name", DataType.Ascii),
                              ("index_type", DataType.Ascii),
                              ("index_options", DataType.Ascii)
                          },
                          rows => rows.WithRows(
                              columns
                                  .Select(col =>
                                      new object[]
                                      {
                                          keyspace, table, col.name, "none", SystemTablePrimeHelper.ByteArrayToString(Encoding.UTF8.GetBytes(col.name)), col.kind, 0,
                                          col.type, "validator", null, null, null
                                      })
                                  .ToArray())));
        }
    }
}