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

using System.Collections.Generic;
using System.Linq;
using System.Text;

using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

namespace Cassandra.IntegrationTests.SimulacronAPI.SystemTables
{
    public static class SystemTablePrimeHelper
    {
        public static void PrimeSystemSchemaKeyspaceV2(this SimulacronBase simulacronCluster, string keyspace)
        {
            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("replication", DataType.Map(DataType.Ascii, DataType.Ascii)),
                              ("keyspace_name", DataType.Ascii),
                              ("durable_writes", DataType.Boolean)
                          },
                          rows => rows.WithRow(new { @class = "SimpleStrategy", replication_factor = "1" }, keyspace, true)));

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
        }

        public static void PrimeSystemSchemaColumnsV2(
            this SimulacronBase simulacronCluster, string keyspace, string table, IEnumerable<StubTableColumn> columns)
        {
            var pkIndex = 0;
            var ckIndex = 0;
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
                                  {
                                      var position = -1;
                                      if (col.Kind == StubColumnKind.ClusteringKey)
                                      {
                                          position = ckIndex;
                                          ckIndex++;
                                      }
                                      else if (col.Kind == StubColumnKind.PartitionKey)
                                      {
                                          position = pkIndex;
                                          pkIndex++;
                                      }

                                      return new object[]
                                      {
                                          keyspace, table, col.Name, col.ClusteringOrder.Value,
                                          DataType.ByteArrayToString(Encoding.UTF8.GetBytes(col.Name)), col.Kind.Value, position,
                                          col.Type.Value
                                      };
                                  })
                                  .ToArray())));
        }

        public static void PrimeSystemSchemaTablesV2(this SimulacronBase simulacronCluster, string keyspace, string table, IEnumerable<StubTableColumn> columns)
        {
            simulacronCluster.PrimeSystemSchemaKeyspaceV2(keyspace);

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

            simulacronCluster.PrimeSystemSchemaColumnsV2(keyspace, table, columns);

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
        }

        public static void PrimeSystemSchemaKeyspaceV1(this SimulacronBase simulacronCluster, string keyspace)
        {
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
                b => b.WhenQuery($"SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '{keyspace}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("strategy_options", DataType.Ascii),
                              ("strategy_class", DataType.Ascii),
                              ("keyspace_name", DataType.Ascii),
                              ("durable_writes", DataType.Boolean)
                          },
                          rows => rows.WithRow("{\"replication_factor\":\"1\"}", "SimpleStrategy", keyspace, true)));
        }

        public static void PrimeSystemSchemaColumnsV1(
            this SimulacronBase simulacronCluster, string keyspace, string table, IEnumerable<StubTableColumn> columns)
        {
            var pkIndex = 0;
            var ckIndex = 0;
            simulacronCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM system.schema_columns WHERE columnfamily_name='{table}' AND keyspace_name='{keyspace}'")
                      .ThenRowsSuccess(new[]
                          {
                              ("keyspace_name", DataType.Ascii),
                              ("columnfamily_name", DataType.Ascii),
                              ("column_name", DataType.Ascii),
                              ("component_index", DataType.Int),
                              ("type", DataType.Ascii),
                              ("validator", DataType.Ascii),
                              ("index_name", DataType.Ascii),
                              ("index_type", DataType.Ascii),
                              ("index_options", DataType.Ascii)
                          },
                          rows => rows.WithRows(
                              columns
                                  .Select(col =>
                                  {
                                      var position = -1;
                                      if (col.Kind == StubColumnKind.ClusteringKey)
                                      {
                                          position = ckIndex;
                                          ckIndex++;
                                      }
                                      else if (col.Kind == StubColumnKind.PartitionKey)
                                      {
                                          position = pkIndex;
                                          pkIndex++;
                                      }

                                      return new object[]
                                      {
                                          keyspace, table, col.Name,
                                          position,
                                          col.Kind.Value, col.Type.GetFqTypeName(), null, null, null
                                      };
                                  })
                                  .ToArray())));
        }

        public static void PrimeSystemSchemaTablesV1(this SimulacronBase simulacronCluster, string keyspace, string table, IEnumerable<StubTableColumn> columns)
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

            simulacronCluster.PrimeSystemSchemaKeyspaceV1(keyspace);

            simulacronCluster.PrimeSystemSchemaColumnsV1(keyspace, table, columns);
        }

        public static void PrimeSystemSchemaUdtV2(this SimulacronBase simulacron, string keyspace, string type, IEnumerable<StubUdtField> fields)
        {
            var names = fields.Select(c => c.Name);
            var types = fields.Select(c => c.Type.Value);
            simulacron.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT * FROM system_schema.types WHERE keyspace_name='{keyspace}' AND type_name = '{type}'")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("field_names", DataType.Frozen(DataType.List(DataType.Text))),
                              ("field_types", DataType.Frozen(DataType.List(DataType.Text))),
                              ("keyspace_name", DataType.Text),
                              ("type_name", DataType.Text)
                          },
                          rows =>
                              rows.WithRow(names, types, keyspace, type)));
        }
    }
}