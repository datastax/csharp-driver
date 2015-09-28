using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra
{
    internal abstract class SchemaParser
    {
        private const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        protected abstract string SelectAggregates { get; }
        protected abstract string SelectFunctions { get; }
        protected abstract string SelectUdts { get; }

        /// <summary>
        /// Gets the keyspace metadata
        /// </summary>
        /// <returns>The keyspace metadata or null if not found</returns>
        public abstract Task<KeyspaceMetadata> GetKeyspace(IMetadataQueryProvider cc, string name);

        /// <summary>
        /// Gets all the keyspaces metadata
        /// </summary>
        public abstract Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(IMetadataQueryProvider cc, bool retry);

        public abstract Task<TableMetadata> GetTable(IMetadataQueryProvider cc, string keyspaceName, string tableName);

        public abstract Task<ICollection<string>> GetTableNames(IMetadataQueryProvider cc, string keyspaceName);

        public Task<FunctionMetadata> GetFunction(IMetadataQueryProvider cc, string keyspaceName, string functionName, string signatureString)
        {
            var query = string.Format(SelectFunctions, keyspaceName, functionName, signatureString);
            return cc
                .QueryAsync(query, true)
                .ContinueSync(rs =>
                {
                    var row = rs.FirstOrDefault();
                    return row != null ? FunctionMetadata.Build(row) : null;
                });
        }

        public Task<AggregateMetadata> GetAggregate(IMetadataQueryProvider cc, string keyspaceName, string aggregateName, string signatureString)
        {
            var query = string.Format(SelectAggregates, keyspaceName, aggregateName, signatureString);
            return cc
                .QueryAsync(query, true)
                .ContinueSync(rs =>
                {
                    var row = rs.FirstOrDefault();
                    return row != null ? AggregateMetadata.Build(cc.ProtocolVersion, row) : null;
                });
        }

        public Task<UdtColumnInfo> GetUdtDefinition(IMetadataQueryProvider cc, string keyspaceName, string typeName)
        {
            return cc
                .QueryAsync(string.Format(SelectUdts, keyspaceName, typeName), true)
                .ContinueSync(rs =>
                {
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
                });
        }

        protected static ColumnDesc[] AdaptKeyTypes(string typesString)
        {
            if (typesString == null)
            {
                return new ColumnDesc[0];
            }
            var indexes = new List<int>();
            for (var i = 1; i < typesString.Length; i++)
            {
                if (typesString[i] == ',') 
                {
                    indexes.Add(i + 1);
                }
            }
            if (typesString.StartsWith(CompositeTypeName))
            {
                indexes.Insert(0, CompositeTypeName.Length + 1);
                indexes.Add(typesString.Length);
            }
            else
            {
                indexes.Insert(0, 0);
                //we are talking about indexes
                //the next valid start indexes would be at length + 1
                indexes.Add(typesString.Length + 1);
            }
            var types = new ColumnDesc[indexes.Count - 1];
            for (var i = 0; i < types.Length; i++) 
            {
                types[i] = TypeCodec.ParseDataType(typesString, indexes[i], indexes[i + 1] - indexes[i] - 1);
            }
            return types;
        }
    }

    /// <summary>
    /// Schema parser for metadata tables for Cassandra versions 2.2 or below
    /// </summary>
    internal class SchemaParserV1: SchemaParser
    {
        /// <summary>
        /// The single instance of SchemaParser implementation v1
        /// </summary>
        public static readonly SchemaParserV1 Instance = new SchemaParserV1();

        private const string SelectColumns = "SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectSingleTable = "SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const string SelectTables = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='{0}'";

        protected override string SelectAggregates
        {
            get { return "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '{0}' AND aggregate_name = '{1}' AND signature = {2}"; }
        }

        protected override string SelectFunctions
        {
            get { return "SELECT * FROM system.schema_functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND signature = {2}"; }
        }

        protected override string SelectUdts
        {
            get { return "SELECT * FROM system.schema_usertypes WHERE keyspace_name='{0}' AND type_name = '{1}'"; }
        }

        private SchemaParserV1()
        {
            
        }

        private KeyspaceMetadata ParseKeyspaceRow(IMetadataQueryProvider cc, Row row)
        {
            if (row == null)
            {
                return null;
            }
            return new KeyspaceMetadata(
                this,
                cc,
                row.GetValue<string>("keyspace_name"),
                row.GetValue<bool>("durable_writes"),
                row.GetValue<string>("strategy_class"),
                Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options")));
        }

        public override Task<KeyspaceMetadata> GetKeyspace(IMetadataQueryProvider cc, string name)
        {
            return cc
                .QueryAsync(String.Format(SelectSingleKeyspace, name), true)
                .ContinueSync(rs => ParseKeyspaceRow(cc, rs.FirstOrDefault()));
        }

        public override Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(IMetadataQueryProvider cc, bool retry)
        {
            return cc
                .QueryAsync(SelectKeyspaces, retry)
                .ContinueSync(rs => rs.Select(r => ParseKeyspaceRow(cc, r)));
        }

        private static SortedDictionary<string, string> GetCompactionStrategyOptions(Row row)
        {
            var result = new SortedDictionary<string, string> { { "class", row.GetValue<string>("compaction_strategy_class") } };
            foreach (var entry in Utils.ConvertStringToMap(row.GetValue<string>("compaction_strategy_options")))
            {
                result.Add(entry.Key, entry.Value);
            }
            return result;
        }

        public override Task<TableMetadata> GetTable(IMetadataQueryProvider cc, string keyspaceName, string tableName)
        {
            var columns = new Dictionary<string, TableColumn>();
            var partitionKeys = new List<Tuple<int, TableColumn>>();
            var clusteringKeys = new List<Tuple<int, TableColumn>>();
            return cc
                .QueryAsync(string.Format(SelectSingleTable, tableName, keyspaceName), true)
                .Then(rs =>
                {
                    var tableMetadataRow = rs.FirstOrDefault();
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
                            (SortedDictionary<string, string>) Utils.ConvertStringToMap(tableMetadataRow.GetValue<string>("compression_parameters"))
                    };
                    //replicate_on_write column not present in C* >= 2.1
                    if (tableMetadataRow.GetColumn("replicate_on_write") != null)
                    {
                        options.replicateOnWrite = tableMetadataRow.GetValue<bool>("replicate_on_write");
                    }
                    return cc
                        .QueryAsync(string.Format(SelectColumns, tableName, keyspaceName), true)
                        .ContinueSync(columnsMetadata =>
                        {
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
                                if (row.GetColumn("type") != null)
                                {
                                    switch (row.GetValue<string>("type"))
                                    {
                                        case "partition_key":
                                            partitionKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, col));
                                            break;
                                        case "clustering_key":
                                            clusteringKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, col));
                                            break;
                                    }
                                }
                                columns.Add(col.Name, col);
                            }
                            var comparator = tableMetadataRow.GetValue<string>("comparator");
                            if (tableMetadataRow.GetColumn("key_aliases") != null && partitionKeys.Count == 0)
                            {
                                //In C* 1.2, keys are not stored on the schema_columns table
                                var partitionKeyNames = Utils.ParseJsonStringArray(tableMetadataRow.GetValue<string>("key_aliases"));
                                var types = AdaptKeyTypes(tableMetadataRow.GetValue<string>("key_validator"));
                                for (var i = 0; i < partitionKeyNames.Length; i++)
                                {
                                    var name = partitionKeyNames[i];
                                    TableColumn c;
                                    if (!columns.TryGetValue(name, out c))
                                    {
                                        c = new TableColumn
                                        {
                                            Name = name,
                                            Keyspace = keyspaceName,
                                            Table = tableName,
                                            TypeCode = types[i].TypeCode,
                                            TypeInfo = types[i].TypeInfo,
                                            KeyType = KeyType.Partition
                                        };
                                        //The column is not part of columns metadata table
                                        columns.Add(name, c);
                                    }
                                    partitionKeys.Add(Tuple.Create(i, c));
                                }
                                //In C* 1.2, keys are not stored on the schema_columns table
                                var clusteringKeyNames = Utils.ParseJsonStringArray(tableMetadataRow.GetValue<string>("column_aliases"));
                                types = AdaptKeyTypes(comparator);
                                for (var i = 0; i < clusteringKeyNames.Length; i++)
                                {
                                    var name = clusteringKeyNames[i];
                                    TableColumn c;
                                    if (!columns.TryGetValue(name, out c))
                                    {
                                        c = new TableColumn
                                        {
                                            Name = name,
                                            Keyspace = keyspaceName,
                                            Table = tableName,
                                            TypeCode = types[i].TypeCode,
                                            TypeInfo = types[i].TypeInfo,
                                            KeyType = KeyType.Clustering
                                        };
                                        //The column is not part of columns metadata table
                                        columns.Add(name, c);
                                    }
                                    clusteringKeys.Add(Tuple.Create(i, c));
                                }
                            }
                            options.isCompactStorage = tableMetadataRow.GetColumn("is_dense") != null && tableMetadataRow.GetValue<bool>("is_dense");
                            if (!options.isCompactStorage)
                            {
                                //is_dense column does not exist in previous versions of Cassandra
                                //also, compact pk, ck and val appear as is_dense false
                                // clusteringKeys != comparator types - 1
                                // or not composite (comparator)
                                options.isCompactStorage = !comparator.StartsWith(TypeCodec.CompositeTypeName);
                            }

                            return new TableMetadata(
                                tableName, columns.Values.ToArray(),
                                partitionKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                                clusteringKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                                options);
                        });
                });
        }

        public override Task<ICollection<string>> GetTableNames(IMetadataQueryProvider cc, string keyspaceName)
        {
            return cc
                .QueryAsync(string.Format(SelectTables, keyspaceName), true)
                .ContinueSync(rs => (ICollection<string>) rs.Select(r => r.GetValue<string>("columnfamily_name")).ToArray());
        }
    }
}
