//
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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Cassandra
{
    public class KeyspaceMetadata
    {
        private const String SelectSingleTable = "SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const String SelectTables = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='{0}'";
        private const String SelectColumns = "SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const String SelectUdts = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='{0}' AND type_name = '{1}'";
        private const String SelectFunctions = "SELECT * FROM system.schema_functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND signature = {2}";
        private readonly ConcurrentDictionary<string, TableMetadata> _tables = new ConcurrentDictionary<string, TableMetadata>();
        private readonly ConcurrentDictionary<Tuple<string, string[]>, FunctionMetadata> _functions = new ConcurrentDictionary<Tuple<string, string[]>, FunctionMetadata>();
        private readonly ControlConnection _cc;

        /// <summary>
        ///  Gets the name of this keyspace.
        /// </summary>
        /// <returns>the name of this CQL keyspace.</returns>
        public string Name { get; private set; }

        /// <summary>
        ///  Gets a value indicating whether durable writes are set on this keyspace.
        /// </summary>
        /// <returns><c>true</c> if durable writes are set on this keyspace
        ///  , <c>false</c> otherwise.</returns>
        public bool DurableWrites { get; private set; }

        /// <summary>
        ///  Gets the Strategy Class of this keyspace.
        /// </summary>
        /// <returns>name of StrategyClass of this keyspace.</returns>
        public string StrategyClass { get; private set; }

        /// <summary>
        ///  Returns the replication options for this keyspace.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the keyspace replication strategy options.</returns>
        public IDictionary<string, int> Replication { get; private set; }

        internal KeyspaceMetadata(ControlConnection cc, string name, bool durableWrites, string strategyClass,
                                  IDictionary<string, int> replicationOptions)
        {
            _cc = cc;
            Name = name;
            DurableWrites = durableWrites;

            StrategyClass = strategyClass;
            if (strategyClass != null && strategyClass.StartsWith("org.apache.cassandra.locator."))
            {
                StrategyClass = strategyClass.Replace("org.apache.cassandra.locator.", "");   
            }
            Replication = replicationOptions;
        }


        /// <summary>
        ///  Returns metadata of specified table in this keyspace.
        /// </summary>
        /// <param name="tableName"> the name of table to retrieve </param>
        /// <returns>the metadata for table <c>tableName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public TableMetadata GetTableMetadata(string tableName)
        {
            TableMetadata table;
            if (_tables.TryGetValue(tableName, out table))
            {
                //The table metadata is available in local cache
                return table;
            }
            var keyspaceName = Name;
            var columns = new Dictionary<string, TableColumn>();
            var partitionKeys = new List<Tuple<int, TableColumn>>();
            var tableMetadataRow = _cc.Query(String.Format(SelectSingleTable, tableName, keyspaceName), true).FirstOrDefault();
            if (tableMetadataRow == null)
            {
                return null;
            }
            //Read table options
            var options = new TableOptions
            {
                isCompactStorage = false,
                bfFpChance = tableMetadataRow.GetValue<double>("bloom_filter_fp_chance"),
                caching = tableMetadataRow.GetValue<string>("caching"),
                comment = tableMetadataRow.GetValue<string>("comment"),
                gcGrace = tableMetadataRow.GetValue<int>("gc_grace_seconds"),
                localReadRepair = tableMetadataRow.GetValue<double>("local_read_repair_chance"),
                readRepair = tableMetadataRow.GetValue<double>("read_repair_chance"),
                compactionOptions = GetCompactionStrategyOptions(tableMetadataRow),
                compressionParams =
                    (SortedDictionary<string, string>)Utils.ConvertStringToMap(tableMetadataRow.GetValue<string>("compression_parameters"))
            };
            //replicate_on_write column not present in C* >= 2.1
            if (tableMetadataRow.GetColumn("replicate_on_write") != null)
            {
                options.replicateOnWrite = tableMetadataRow.GetValue<bool>("replicate_on_write");
            }

            var columnsMetadata = _cc.Query(String.Format(SelectColumns, tableName, keyspaceName), true);
            foreach (var row in columnsMetadata)
            {
                var dataType = TypeCodec.ParseDataType(row.GetValue<string>("validator"));
                var col = new TableColumn
                {
                    Name = row.GetValue<string>("column_name"),
                    Keyspace = row.GetValue<string>("keyspace_name"),
                    Table = row.GetValue<string>("columnfamily_name"),
                    TypeCode = dataType.TypeCode,
                    TypeInfo = dataType.TypeInfo,
                    SecondaryIndexName = row.GetValue<string>("index_name"),
                    SecondaryIndexType = row.GetValue<string>("index_type"),
                    KeyType =
                        row.GetValue<string>("index_name") != null
                            ? KeyType.SecondaryIndex
                            : KeyType.None,
                };
                if (row.GetColumn("type") != null && row.GetValue<string>("type") == "partition_key")
                {
                    partitionKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, col));
                }
                columns.Add(col.Name, col);
            }
            var comparator = tableMetadataRow.GetValue<string>("comparator");
            var comparatorComposite = false;
            if (comparator.StartsWith(TypeCodec.CompositeTypeName))
            {
                comparator = comparator.Replace(TypeCodec.CompositeTypeName, "");
                comparatorComposite = true;
            }
            //Remove reversed type
            comparator = comparator.Replace(TypeCodec.ReversedTypeName, "");
            if (partitionKeys.Count == 0 && tableMetadataRow.GetColumn("key_aliases") != null)
            {
                //In C* 1.2, keys are not stored on the schema_columns table
                var colNames = tableMetadataRow.GetValue<string>("column_aliases");
                var rowKeys = colNames.Substring(1, colNames.Length - 2).Split(',');
                for (var i = 0; i < rowKeys.Length; i++)
                {
                    if (rowKeys[i].StartsWith("\""))
                    {
                        rowKeys[i] = rowKeys[i].Substring(1, rowKeys[i].Length - 2).Replace("\"\"", "\"");
                    }
                }
                if (rowKeys.Length > 0 && rowKeys[0] != string.Empty)
                {
                    var rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+");
                    var rowKeysTypes = rg.Matches(comparator);

                    for (var i = 0; i < rowKeys.Length; i++)
                    {
                        var keyName = rowKeys[i];
                        var dataType = TypeCodec.ParseDataType(rowKeysTypes[i].ToString());
                        var dsc = new TableColumn
                        {
                            Name = keyName,
                            Keyspace = tableMetadataRow.GetValue<string>("keyspace_name"),
                            Table = tableMetadataRow.GetValue<string>("columnfamily_name"),
                            TypeCode = dataType.TypeCode,
                            TypeInfo = dataType.TypeInfo,
                            KeyType = KeyType.Clustering,
                        };
                        columns[dsc.Name] = dsc;
                    }
                }
                var keys = tableMetadataRow.GetValue<string>("key_aliases")
                    .Replace("[", "")
                    .Replace("]", "")
                    .Split(',');
                var keyTypes = tableMetadataRow.GetValue<string>("key_validator")
                    .Replace("org.apache.cassandra.db.marshal.CompositeType", "")
                    .Replace("(", "")
                    .Replace(")", "")
                    .Split(',');
                
                
                for (var i = 0; i < keys.Length; i++)
                {
                    var name = keys[i].Replace("\"", "").Trim();
                    var dataType = TypeCodec.ParseDataType(keyTypes[i].Trim());
                    var c = new TableColumn()
                    {
                        Name = name,
                        Keyspace = tableMetadataRow.GetValue<string>("keyspace_name"),
                        Table = tableMetadataRow.GetValue<string>("columnfamily_name"),
                        TypeCode = dataType.TypeCode,
                        TypeInfo = dataType.TypeInfo,
                        KeyType = KeyType.Partition
                    };
                    columns[name] = c;
                    partitionKeys.Add(Tuple.Create(i, c));
                }
            }

            options.isCompactStorage = tableMetadataRow.GetColumn("is_dense") != null && tableMetadataRow.GetValue<bool>("is_dense");
            if (!options.isCompactStorage)
            {
                //is_dense column does not exist in previous versions of Cassandra
                //also, compact pk, ck and val appear as is_dense false
                // clusteringKeys != comparator types - 1
                // or not composite (comparator)
                options.isCompactStorage = !comparatorComposite;
            }

            table = new TableMetadata(
                tableName, columns.Values.ToArray(), 
                partitionKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(), 
                options);
            //Cache it
            _tables.AddOrUpdate(tableName, table, (k, o) => table);
            return table;
        }

        /// <summary>
        /// Removes table metadata forcing refresh the next time the table metadata is retrieved
        /// </summary>
        internal void ClearTableMetadata(string tableName)
        {
            TableMetadata table;
            _tables.TryRemove(tableName, out table);
        }

        private SortedDictionary<string, string> GetCompactionStrategyOptions(Row row)
        {
            var result = new SortedDictionary<string, string> { { "class", row.GetValue<string>("compaction_strategy_class") } };
            foreach (var entry in Utils.ConvertStringToMap(row.GetValue<string>("compaction_strategy_options")))
            {
                result.Add(entry.Key, entry.Value);
            }
            return result;
        }

        /// <summary>
        ///  Returns metadata of all tables defined in this keyspace.
        /// </summary>
        /// <returns>an IEnumerable of TableMetadata for the tables defined in this
        ///  keyspace.</returns>
        public IEnumerable<TableMetadata> GetTablesMetadata()
        {
            var tableNames = GetTablesNames();
            return tableNames.Select(GetTableMetadata);
        }


        /// <summary>
        ///  Returns names of all tables defined in this keyspace.
        /// </summary>
        /// 
        /// <returns>a collection of all, defined in this
        ///  keyspace tables names.</returns>
        public ICollection<string> GetTablesNames()
        {
            return _cc.Query(String.Format(SelectTables, Name), true).Select(r => r.GetValue<string>("columnfamily_name")).ToList();
        }

        /// <summary>
        ///  Return a <c>String</c> containing CQL queries representing this
        ///  name and the table it contains. In other words, this method returns the
        ///  queries that would allow to recreate the schema of this name, along with
        ///  all its table. Note that the returned String is formatted to be human
        ///  readable (for some definition of human readable at least).
        /// </summary>
        /// <returns>the CQL queries representing this name schema as a code
        ///  String}.</returns>
        public string ExportAsString()
        {
            var sb = new StringBuilder();

            sb.Append(AsCqlQuery()).Append("\n");

            return sb.ToString();
        }


        /// <summary>
        ///  Returns a CQL query representing this keyspace. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </summary>
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.
        ///  <see>#ExportAsString</see></returns>
        public string AsCqlQuery()
        {
            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.QuoteIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(Replication["class"]).Append("'");
            foreach (var rep in Replication)
            {
                if (rep.Key == "class")
                {
                    continue;
                }
                sb.Append(", '").Append(rep.Key).Append("': '").Append(rep.Value).Append("'");
            }
            sb.Append(" } AND DURABLE_WRITES = ").Append(DurableWrites);
            sb.Append(";");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the definition of a User defined type
        /// </summary>
        internal UdtColumnInfo GetUdtDefinition(string typeName)
        {
            var keyspaceName = Name;
            var rs = _cc.Query(String.Format(SelectUdts, keyspaceName, typeName), true);
            var row = rs.FirstOrDefault();
            if (row == null)
            {
                return null;
            }
            var udt = new UdtColumnInfo(row.GetValue<string>("keyspace_name") + "." + row.GetValue<string>("type_name"));
            var fieldNames = row.GetValue<List<string>>("field_names");
            var fieldTypes = row.GetValue<List<string>>("field_types");
            for (var i = 0; i < fieldNames.Count && i < fieldTypes.Count; i++)
            {
                var field = TypeCodec.ParseDataType(fieldTypes[i]);
                field.Name = fieldNames[i];
                udt.Fields.Add(field);
            }
            return udt;
        }

        /// <summary>
        /// Gets a CQL function by name and signature
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string functionName, string[] signature)
        {
            if (signature == null)
            {
                signature = new string[0];
            }
            FunctionMetadata func;
            var key = Tuple.Create(functionName, signature);
            if (_functions.TryGetValue(key, out func))
            {
                return func;
            }
            //Try to retrieve
            var signatureString = "[" + String.Join(",", signature) + "]";
            var row = _cc.Query(String.Format(SelectFunctions, Name, functionName, signatureString), true).FirstOrDefault();
            if (row == null)
            {
                return null;
            }
            func = FunctionMetadata.Build(row);
            _functions.AddOrUpdate(key, func, (k, v) => func);
            return func;
        }
    }
}
