using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cassandra.Tasks;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra
{
    internal abstract class SchemaParser
    {
        private static readonly Version Version30 = new Version(3, 0);

        /// <summary>
        /// Creates a new instance if the currentInstance is not valid for the given Cassandra version
        /// </summary>
        public static SchemaParser GetInstance(Version cassandraVersion, Metadata parent, Func<string, string, Task<UdtColumnInfo>> udtResolver, SchemaParser currentInstance = null)
        {
            if (cassandraVersion >= Version30 && !(currentInstance is SchemaParserV2))
            {
                return new SchemaParserV2(parent, udtResolver);
            }
            if (cassandraVersion < Version30 && !(currentInstance is SchemaParserV1))
            {
                return new SchemaParserV1(parent);
            }
            if (currentInstance == null)
            {
                throw new ArgumentNullException("currentInstance");
            }
            return currentInstance;
        }

        protected const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        protected readonly IMetadataQueryProvider Cc;
        protected readonly Metadata Parent;
        protected abstract string SelectAggregates { get; }
        protected abstract string SelectFunctions { get; }
        protected abstract string SelectTables { get; }
        protected abstract string SelectUdts { get; }

        protected SchemaParser(Metadata parent)
        {
            Cc = parent.ControlConnection;
            Parent = parent;
        }

        /// <summary>
        /// Gets the keyspace metadata
        /// </summary>
        /// <returns>The keyspace metadata or null if not found</returns>
        public abstract Task<KeyspaceMetadata> GetKeyspace(string name);

        /// <summary>
        /// Gets all the keyspaces metadata
        /// </summary>
        public abstract Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(bool retry);

        public abstract Task<TableMetadata> GetTable(string keyspaceName, string tableName);

        public abstract Task<MaterializedViewMetadata> GetView(string keyspaceName, string viewName);

        public Task<ICollection<string>> GetTableNames(string keyspaceName)
        {
            return Cc
                .QueryAsync(string.Format(SelectTables, keyspaceName), true)
                .ContinueSync(rs => (ICollection<string>)rs.Select(r => r.GetValue<string>(0)).ToArray());
        }

        public abstract Task<FunctionMetadata> GetFunction(string keyspaceName, string functionName, string signatureString);

        public abstract Task<AggregateMetadata> GetAggregate(string keyspaceName, string aggregateName, string signatureString);

        public abstract Task<UdtColumnInfo> GetUdtDefinition(string keyspaceName, string typeName);
    }

    /// <summary>
    /// Schema parser for metadata tables for Cassandra versions 2.2 or below
    /// </summary>
    internal class SchemaParserV1: SchemaParser
    {
        private static readonly Task<TableMetadata> NullTableTask = TaskHelper.ToTask((TableMetadata)null);
        private const string SelectColumns = "SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectSingleTable = "SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";

        protected override string SelectAggregates
        {
            get { return "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '{0}' AND aggregate_name = '{1}' AND signature = {2}"; }
        }

        protected override string SelectFunctions
        {
            get { return "SELECT * FROM system.schema_functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND signature = {2}"; }
        }

        protected override string SelectTables
        {
            get { return "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='{0}'"; }
        }

        protected override string SelectUdts
        {
            get { return "SELECT * FROM system.schema_usertypes WHERE keyspace_name='{0}' AND type_name = '{1}'"; }
        }

        internal SchemaParserV1(Metadata parent) : base(parent)
        {
            
        }

        private KeyspaceMetadata ParseKeyspaceRow(Row row)
        {
            if (row == null)
            {
                return null;
            }
            return new KeyspaceMetadata(
                Parent,
                row.GetValue<string>("keyspace_name"),
                row.GetValue<bool>("durable_writes"),
                row.GetValue<string>("strategy_class"),
                Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options")));
        }

        public override Task<KeyspaceMetadata> GetKeyspace(string name)
        {
            return Cc
                .QueryAsync(string.Format(SelectSingleKeyspace, name), true)
                .ContinueSync(rs => ParseKeyspaceRow(rs.FirstOrDefault()));
        }

        public override Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(bool retry)
        {
            return Cc
                .QueryAsync(SelectKeyspaces, retry)
                .ContinueSync(rs => rs.Select(ParseKeyspaceRow));
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

        public override Task<TableMetadata> GetTable(string keyspaceName, string tableName)
        {
            var columns = new Dictionary<string, TableColumn>();
            var partitionKeys = new List<Tuple<int, TableColumn>>();
            var clusteringKeys = new List<Tuple<int, Tuple<TableColumn, SortOrder>>>();
            return Cc
                .QueryAsync(string.Format(SelectSingleTable, tableName, keyspaceName), true)
                .Then(rs =>
                {
                    var tableMetadataRow = rs.FirstOrDefault();
                    if (tableMetadataRow == null)
                    {
                        return NullTableTask;
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
                    return Cc
                        .QueryAsync(string.Format(SelectColumns, tableName, keyspaceName), true)
                        .ContinueSync(columnsMetadata =>
                        {
                            foreach (var row in columnsMetadata)
                            {
                                var dataType = TypeCodec.ParseFqTypeName(row.GetValue<string>("validator"));
                                var col = new TableColumn
                                {
                                    Name = row.GetValue<string>("column_name"),
                                    Keyspace = row.GetValue<string>("keyspace_name"),
                                    Table = row.GetValue<string>("columnfamily_name"),
                                    TypeCode = dataType.TypeCode,
                                    TypeInfo = dataType.TypeInfo,
                                    #pragma warning disable 618
                                    SecondaryIndexName = row.GetValue<string>("index_name"),
                                    SecondaryIndexType = row.GetValue<string>("index_type"),
                                    SecondaryIndexOptions = Utils.ParseJsonStringMap(row.GetValue<string>("index_options")),
                                    #pragma warning restore 618
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
                                        {
                                            var sortOrder = dataType.IsReversed ? SortOrder.Descending : SortOrder.Ascending;
                                            clusteringKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, Tuple.Create(col, sortOrder)));
                                            break;
                                        }
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
                                if (clusteringKeyNames.Length > 0)
                                {
                                    types = AdaptKeyTypes(comparator);
                                    for (var i = 0; i < clusteringKeyNames.Length; i++)
                                    {
                                        var name = clusteringKeyNames[i];
                                        TableColumn c;
                                        var dataType = types[i];
                                        if (!columns.TryGetValue(name, out c))
                                        {
                                            c = new TableColumn
                                            {
                                                Name = name,
                                                Keyspace = keyspaceName,
                                                Table = tableName,
                                                TypeCode = dataType.TypeCode,
                                                TypeInfo = dataType.TypeInfo,
                                                KeyType = KeyType.Clustering
                                            };
                                            //The column is not part of columns metadata table
                                            columns.Add(name, c);
                                        }
                                        clusteringKeys.Add(Tuple.Create(i, Tuple.Create(c, dataType.IsReversed ? SortOrder.Descending : SortOrder.Ascending)));
                                    }
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
                            var result = new TableMetadata(tableName, GetIndexesFromColumns(columns.Values));
                            result.SetValues(
                                columns,
                                partitionKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                                clusteringKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                                options);
                            return result;
                        });
                });
        }

        private static ColumnDesc[] AdaptKeyTypes(string typesString)
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
                types[i] = TypeCodec.ParseFqTypeName(typesString, indexes[i], indexes[i + 1] - indexes[i] - 1);
            }
            return types;
        }

        public override Task<MaterializedViewMetadata> GetView(string keyspaceName, string viewName)
        {
            return TaskHelper.FromException<MaterializedViewMetadata>(new NotSupportedException("Materialized views are supported in Cassandra 3.0 or above"));
        }

        /// <summary>
        /// Gets the index metadata based on the legacy column metadata
        /// </summary>
        private static IDictionary<string, IndexMetadata> GetIndexesFromColumns(IEnumerable<TableColumn> columns)
        {
            //Use obsolete properties
            #pragma warning disable 618
            return columns
                .Where(c => c.SecondaryIndexName != null)
                .Select(IndexMetadata.FromTableColumn)
                .ToDictionary(ix => ix.Name);
            #pragma warning restore 618
        }

        public override Task<UdtColumnInfo> GetUdtDefinition(string keyspaceName, string typeName)
        {
            return Cc
                .QueryAsync(string.Format(SelectUdts, keyspaceName, typeName), true)
                .ContinueSync(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return null;
                    }
                    var udt = new UdtColumnInfo(row.GetValue<string>("keyspace_name") + "." + row.GetValue<string>("type_name"));
                    var fieldNames = row.GetValue<string[]>("field_names");
                    var fieldTypes = row.GetValue<string[]>("field_types");
                    for (var i = 0; i < fieldNames.Length && i < fieldTypes.Length; i++)
                    {
                        var field = TypeCodec.ParseFqTypeName(fieldTypes[i]);
                        field.Name = fieldNames[i];
                        udt.Fields.Add(field);
                    }
                    return udt;
                });
        }

        public override Task<FunctionMetadata> GetFunction(string keyspaceName, string functionName, string signatureString)
        {
            var query = string.Format(SelectFunctions, keyspaceName, functionName, signatureString);
            return Cc
                .QueryAsync(query, true)
                .ContinueSync(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return null;
                    }
                    var emptyArray = new string[0];
                    return new FunctionMetadata
                    {
                        Name = row.GetValue<string>("function_name"),
                        KeyspaceName = row.GetValue<string>("keyspace_name"),
                        Signature = row.GetValue<string[]>("signature") ?? emptyArray,
                        ArgumentNames = row.GetValue<string[]>("argument_names") ?? emptyArray,
                        Body = row.GetValue<string>("body"),
                        CalledOnNullInput = row.GetValue<bool>("called_on_null_input"),
                        Language = row.GetValue<string>("language"),
                        ReturnType = TypeCodec.ParseFqTypeName(row.GetValue<string>("return_type")),
                        ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => TypeCodec.ParseFqTypeName(s)).ToArray()
                    };
                });
        }

        public override Task<AggregateMetadata> GetAggregate(string keyspaceName, string aggregateName, string signatureString)
        {
            var query = string.Format(SelectAggregates, keyspaceName, aggregateName, signatureString);
            return Cc
                .QueryAsync(query, true)
                .ContinueSync(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return null;
                    }

                    var emptyArray = new string[0];
                    var aggregate = new AggregateMetadata
                    {
                        Name = row.GetValue<string>("aggregate_name"),
                        KeyspaceName = row.GetValue<string>("keyspace_name"),
                        Signature = row.GetValue<string[]>("signature") ?? emptyArray,
                        StateFunction = row.GetValue<string>("state_func"),
                        StateType = TypeCodec.ParseFqTypeName(row.GetValue<string>("state_type")),
                        FinalFunction = row.GetValue<string>("final_func"),
                        ReturnType = TypeCodec.ParseFqTypeName(row.GetValue<string>("return_type")),
                        ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => TypeCodec.ParseFqTypeName(s)).ToArray(),
                    };
                    var initConditionRaw = TypeCodec.Decode(Cc.ProtocolVersion, row.GetValue<byte[]>("initcond"), aggregate.StateType.TypeCode, aggregate.StateType.TypeInfo);
                    if (initConditionRaw != null)
                    {
                        aggregate.InitialCondition = initConditionRaw.ToString();   
                    }
                    return aggregate;
                });
        }
    }

    /// <summary>
    /// Schema parser for metadata tables for Cassandra version 3.0 and above
    /// </summary>
    internal class SchemaParserV2 : SchemaParser
    {
        private readonly Func<string, string, Task<UdtColumnInfo>> _udtResolver;

        private const string SelectColumns = "SELECT * FROM system_schema.columns WHERE table_name='{0}' AND keyspace_name='{1}'";
        private const string SelectIndexes = "SELECT * FROM system_schema.indexes WHERE table_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspaces = "SELECT * FROM system_schema.keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectSingleTable = "SELECT * FROM system_schema.tables WHERE table_name='{0}' AND keyspace_name='{1}'";
        private const string SelectSingleView = "SELECT * FROM system_schema.views WHERE view_name='{0}' AND keyspace_name='{1}'";

        protected override string SelectAggregates
        {
            get { return "SELECT * FROM system_schema.aggregates WHERE keyspace_name = '{0}' AND aggregate_name = '{1}' AND argument_types = {2}"; }
        }

        protected override string SelectFunctions
        {
            get { return "SELECT * FROM system_schema.functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND argument_types = {2}"; }
        }

        protected override string SelectTables
        {
            get { return "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{0}'"; }
        }

        protected override string SelectUdts
        {
            get { return "SELECT * FROM system_schema.types WHERE keyspace_name='{0}' AND type_name = '{1}'"; }
        }

        internal SchemaParserV2(Metadata parent, Func<string, string, Task<UdtColumnInfo>> udtResolver)
            : base(parent)
        {
            _udtResolver = udtResolver;
        }

        private KeyspaceMetadata ParseKeyspaceRow(Row row)
        {
            if (row == null)
            {
                return null;
            }
            var replication = row.GetValue<IDictionary<string, string>>("replication");
            string strategy = null;
            Dictionary<string, int> strategyOptions = null;
            if (replication != null) 
            {
                strategy = replication["class"];
                strategyOptions = new Dictionary<string, int>();
                foreach (var kv in replication)
                {
                    if (kv.Key == "class")
                    {
                        continue;
                    }
                    strategyOptions[kv.Key] = Convert.ToInt32(kv.Value);
                }
            }
            return new KeyspaceMetadata(
                Parent,
                row.GetValue<string>("keyspace_name"),
                row.GetValue<bool>("durable_writes"),
                strategy,
                strategyOptions);
        }

        public override Task<KeyspaceMetadata> GetKeyspace(string name)
        {
            return Cc
                .QueryAsync(string.Format(SelectSingleKeyspace, name), true)
                .ContinueSync(rs => ParseKeyspaceRow(rs.FirstOrDefault()));
        }

        public override Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(bool retry)
        {
            return Cc
                .QueryAsync(SelectKeyspaces, retry)
                .ContinueSync(rs => rs.Select(ParseKeyspaceRow));
        }

        public override Task<TableMetadata> GetTable(string keyspaceName, string tableName)
        {
            var getTableTask = Cc.QueryAsync(string.Format(SelectSingleTable, tableName, keyspaceName), true);
            var getColumnsTask = Cc.QueryAsync(string.Format(SelectColumns, tableName, keyspaceName), true);
            var getIndexesTask = Cc.QueryAsync(string.Format(SelectIndexes, tableName, keyspaceName), true);
            return Task.Factory.ContinueWhenAll(new[] {getTableTask, getColumnsTask, getIndexesTask}, tasks =>
            {
                var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                if (ex != null)
                {
                    throw ex.InnerException;
                }
                return ParseTableOrView(_ => new TableMetadata(tableName, GetIndexes(tasks[2].Result)), tasks[0], tasks[1]);
            }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
        }

        public override Task<MaterializedViewMetadata> GetView(string keyspaceName, string viewName)
        {
            var getTableTask = Cc.QueryAsync(string.Format(SelectSingleView, viewName, keyspaceName), true);
            var getColumnsTask = Cc.QueryAsync(string.Format(SelectColumns, viewName, keyspaceName), true);
            return Task.Factory.ContinueWhenAll(new[] { getTableTask, getColumnsTask }, tasks =>
            {
                var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                if (ex != null)
                {
                    throw ex.InnerException;
                }
                return ParseTableOrView(viewRow => new MaterializedViewMetadata(viewName, viewRow.GetValue<string>("where_clause")), tasks[0], tasks[1]);
            }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
        }

        private Task<T> ParseTableOrView<T>(Func<Row, T> newInstance, Task<IEnumerable<Row>> getTableTask, Task<IEnumerable<Row>> getColumnsTask)
            where T : DataCollectionMetadata
        {
            var tableMetadataRow = getTableTask.Result.FirstOrDefault();
            if (tableMetadataRow == null)
            {
                return TaskHelper.ToTask<T>(null);
            }
            var columns = new Dictionary<string, TableColumn>();
            var partitionKeys = new List<Tuple<int, TableColumn>>();
            var clusteringKeys = new List<Tuple<int, Tuple<TableColumn, SortOrder>>>();
            //Read table options
            var options = new TableOptions
            {
                isCompactStorage = false,
                bfFpChance = tableMetadataRow.GetValue<double>("bloom_filter_fp_chance"),
                caching = "{" + string.Join(",", tableMetadataRow.GetValue<IDictionary<string, string>>("caching").Select(kv => "\"" + kv.Key + "\":\"" + kv.Value + "\"")) + "}",
                comment = tableMetadataRow.GetValue<string>("comment"),
                gcGrace = tableMetadataRow.GetValue<int>("gc_grace_seconds"),
                localReadRepair = tableMetadataRow.GetValue<double>("dclocal_read_repair_chance"),
                readRepair = tableMetadataRow.GetValue<double>("read_repair_chance"),
                compactionOptions = tableMetadataRow.GetValue<SortedDictionary<string, string>>("compaction"),
                compressionParams =
                    tableMetadataRow.GetValue<SortedDictionary<string, string>>("compression")
            };
            var columnsMetadata = getColumnsTask.Result;
            Task<Tuple<TableColumn, Row>>[] columnTasks = columnsMetadata
                .Select(row =>
                {
                    return TypeCodec.ParseTypeName(_udtResolver, tableMetadataRow.GetValue<string>("keyspace_name"), row.GetValue<string>("type")).
                        ContinueSync(type => Tuple.Create(new TableColumn
                        {
                            Name = row.GetValue<string>("column_name"),
                            Keyspace = row.GetValue<string>("keyspace_name"),
                            Table = row.GetValue<string>("table_name"),
                            TypeCode = type.TypeCode,
                            TypeInfo = type.TypeInfo
                        }, row));
                }).ToArray();
            return Task.Factory.ContinueWhenAll(columnTasks, tasks =>
            {
                var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                if (ex != null)
                {
                    throw ex.InnerException;
                }
                foreach (var t in tasks)
                {
                    var col = t.Result.Item1;
                    var row = t.Result.Item2;
                    switch (row.GetValue<string>("kind"))
                    {
                        case "partition_key":
                            partitionKeys.Add(Tuple.Create(row.GetValue<int?>("position") ?? 0, col));
                            break;
                        case "clustering":
                            clusteringKeys.Add(Tuple.Create(row.GetValue<int?>("position") ?? 0,
                                Tuple.Create(col, row.GetValue<string>("clustering_order") == "desc" ? SortOrder.Descending : SortOrder.Ascending)));
                            break;
                    }
                    columns.Add(col.Name, col);
                }
                if (typeof(T) == typeof(TableMetadata))
                {
                    var flags = tableMetadataRow.GetValue<string[]>("flags");
                    var isDense = flags.Contains("dense");
                    var isSuper = flags.Contains("super");
                    var isCompound = flags.Contains("compound");
                    options.isCompactStorage = isSuper || isDense || !isCompound;
                    //remove the columns related to Thrift
                    var isStaticCompact = !isSuper && !isDense && !isCompound;
                    if (isStaticCompact)
                    {
                        PruneStaticCompactTableColumns(clusteringKeys, columns);
                    }
                    else if (isDense)
                    {
                        PruneDenseTableColumns(columns);
                    }
                }
                var result = newInstance(tableMetadataRow);
                result.SetValues(columns,
                    partitionKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                    clusteringKeys.OrderBy(p => p.Item1).Select(p => p.Item2).ToArray(),
                    options);
                return result;
            });
        }

        private static void PruneDenseTableColumns(IDictionary<string, TableColumn> columns)
        {
            var columnKeys = columns.Keys.ToArray();
            foreach (var key in columnKeys)
            {
                var c = columns[key];
                if (c.TypeCode == ColumnTypeCode.Custom && c.TypeInfo == null)
                {
                    //empty type
                    columns.Remove(key);
                }
            }
        }

        /// <summary>
        /// Upon migration from thrift to CQL, we internally create a pair of surrogate clustering/regular columns
        /// for compact static tables. These columns shouldn't be exposed to the user but are currently returned by C*.
        /// We also need to remove the static keyword for all other columns in the table.
        /// </summary>
        private static void PruneStaticCompactTableColumns(ICollection<Tuple<int, Tuple<TableColumn, SortOrder>>> clusteringKeys, IDictionary<string, TableColumn> columns)
        {
            //remove "column1 text" clustering column
            foreach (var c in clusteringKeys.Select(t => t.Item2.Item1))
            {
                columns.Remove(c.Name);
            }
            clusteringKeys.Clear();
            //remove regular columns and set the static columns to non-static
            TableColumn valueBlob;
            if (columns.TryGetValue("value", out valueBlob) && valueBlob.TypeCode == ColumnTypeCode.Blob)
            {
                columns.Remove("value");
            }
        }

        private static IDictionary<string, IndexMetadata> GetIndexes(IEnumerable<Row> rows)
        {
            return rows.Select(IndexMetadata.FromRow).ToDictionary(ix => ix.Name);
        }

        public override Task<UdtColumnInfo> GetUdtDefinition(string keyspaceName, string typeName)
        {
            return Cc
                .QueryAsync(string.Format(SelectUdts, keyspaceName, typeName), true)
                .Then(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return TaskHelper.ToTask<UdtColumnInfo>(null);
                    }
                    var udt = new UdtColumnInfo(row.GetValue<string>("keyspace_name") + "." + row.GetValue<string>("type_name"));
                    var fieldTypeTasks = row.GetValue<string[]>("field_types")
                        .Select(name => TypeCodec.ParseTypeName(_udtResolver, keyspaceName, name))
                        .ToArray();
                    return Task.Factory.ContinueWhenAll(fieldTypeTasks, tasks =>
                    {
                        var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                        if (ex != null)
                        {
                            throw ex.InnerException;
                        }
                        var fieldNames = row.GetValue<string[]>("field_names");
                        for (var i = 0; i < fieldNames.Length && i < tasks.Length; i++)
                        {
                            var field = tasks[i].Result;
                            field.Name = fieldNames[i];
                            udt.Fields.Add(field);
                        }
                        return udt;
                    });
                });
        }

        public override Task<AggregateMetadata> GetAggregate(string keyspaceName, string aggregateName, string signatureString)
        {
            var query = string.Format(SelectAggregates, keyspaceName, aggregateName, signatureString);
            return Cc
                .QueryAsync(query, true)
                .Then(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return TaskHelper.ToTask<AggregateMetadata>(null);
                    }
                    var argumentTypes = row.GetValue<string[]>("argument_types") ?? new string[0];
                    //state_type + return_type + amount of argument types
                    var parseTasks = new Task<ColumnDesc>[2 + argumentTypes.Length];
                    parseTasks[0] = TypeCodec.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("state_type"));
                    parseTasks[1] = TypeCodec.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("return_type"));
                    for (var i = 0; i < argumentTypes.Length; i++)
                    {
                        parseTasks[2 + i] = TypeCodec.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), argumentTypes[i]);
                    }
                    return Task.Factory.ContinueWhenAll(parseTasks, tasks =>
                    {
                        var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                        if (ex != null)
                        {
                            throw ex.InnerException;
                        }
                        return new AggregateMetadata
                        {
                            Name = row.GetValue<string>("aggregate_name"),
                            KeyspaceName = row.GetValue<string>("keyspace_name"),
                            StateFunction = row.GetValue<string>("state_func"),
                            FinalFunction = row.GetValue<string>("final_func"),
                            InitialCondition = row.GetValue<string>("initcond"),
                            Signature = argumentTypes,
                            StateType = tasks[0].Result,
                            ReturnType = tasks[1].Result,
                            ArgumentTypes = tasks.Skip(2).Select(t => t.Result).ToArray()
                        };
                    }, TaskContinuationOptions.ExecuteSynchronously);
                });
        }

        public override Task<FunctionMetadata> GetFunction(string keyspaceName, string functionName, string signatureString)
        {
            var query = string.Format(SelectFunctions, keyspaceName, functionName, signatureString);
            return Cc
                .QueryAsync(query, true)
                .Then(rs =>
                {
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        return TaskHelper.ToTask<FunctionMetadata>(null);
                    }
                    var argumentTypes = row.GetValue<string[]>("argument_types") ?? new string[0];
                    var parseTasks = new Task<ColumnDesc>[1 + argumentTypes.Length];
                    parseTasks[0] = TypeCodec.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("return_type"));
                    for (var i = 0; i < argumentTypes.Length; i++)
                    {
                        parseTasks[1 + i] = TypeCodec.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), argumentTypes[i]);
                    }
                    return Task.Factory.ContinueWhenAll(parseTasks, tasks =>
                    {
                        var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                        if (ex != null)
                        {
                            throw ex.InnerException;
                        }
                        return new FunctionMetadata
                        {
                            Name = row.GetValue<string>("function_name"),
                            KeyspaceName = row.GetValue<string>("keyspace_name"),
                            Signature = argumentTypes,
                            ArgumentNames = row.GetValue<string[]>("argument_names") ?? new string[0],
                            Body = row.GetValue<string>("body"),
                            CalledOnNullInput = row.GetValue<bool>("called_on_null_input"),
                            Language = row.GetValue<string>("language"),
                            ReturnType = tasks[0].Result,
                            ArgumentTypes = tasks.Skip(1).Select(t => t.Result).ToArray()
                        };
                    });
                });
        }
    }
}
