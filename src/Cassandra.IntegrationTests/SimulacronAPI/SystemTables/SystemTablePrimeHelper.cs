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
using Cassandra.IntegrationTests.Core;
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
                              ("compression", "map<ascii, ascii>"),
                              ("compaction", "map<ascii, ascii>"),
                              ("bloom_filter_fp_chance", "double"),
                              ("caching", "map<ascii, ascii>"),
                              ("comment", "ascii"),
                              ("gc_grace_seconds", "int"),
                              ("dclocal_read_repair_chance", "double"),
                              ("read_repair_chance", "double"),
                              ("keyspace_name", "ascii")
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
                              ("replication", "map<ascii, ascii>"),
                              ("keyspace_name", "ascii"),
                              ("durable_writes", "boolean")
                          },
                          rows => rows.WithRow(new { @class = "SimpleStrategy", replication_factor = "1" }, keyspace, true)));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system_schema.indexes WHERE table_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("keyspace_name", "ascii"),
                              ("table_name", "ascii"),
                              ("index_name", "ascii"),
                              ("kind", "ascii"),
                              ("options", "map<ascii,ascii>")
                          }));

            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system_schema.columns WHERE table_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(new[]
                          {
                              ("keyspace_name", "ascii"),
                              ("table_name", "ascii"),
                              ("column_name", "ascii"),
                              ("clustering_order", "ascii"),
                              ("column_name_bytes", "blob"),
                              ("kind", "ascii"),
                              ("position", "int"),
                              ("type", "ascii")
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
                              ("compression", "ascii"),
                              ("compression_parameters", "ascii"),
                              ("compaction_strategy_class",  "ascii"),
                              ("compaction_strategy_options", "ascii"),
                              ("bloom_filter_fp_chance", "double"),
                              ("caching", "ascii"),
                              ("comment", "ascii"),
                              ("gc_grace_seconds", "int"),
                              ("dclocal_read_repair_chance", "double"),
                              ("read_repair_chance", "double"),
                              ("keyspace_name", "ascii"),
                              ("local_read_repair_chance", "double"),
                              ("comparator", "ascii")
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
                              ("strategy_options", "ascii"),
                              ("strategy_class", "ascii"),
                              ("keyspace_name", "ascii"),
                              ("durable_writes", "boolean")
                          },
                          rows => rows.WithRow("{\"replication_factor\":\"1\"}", "SimpleStrategy", keyspace, true)));
            
            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system.schema_columns WHERE columnfamily_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(new[]
                          {
                              ("keyspace_name", "ascii"),
                              ("columnfamily_name", "ascii"),
                              ("column_name", "ascii"),
                              ("clustering_order", "ascii"),
                              ("column_name_bytes", "blob"),
                              ("kind", "ascii"),
                              ("position", "int"),
                              ("type", "ascii"),
                              ("validator", "ascii"),
                              ("index_name", "ascii"),
                              ("index_type", "ascii"),
                              ("index_options", "ascii")
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