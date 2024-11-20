//
//      Copyright (C) DataStax Inc.
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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.MetadataHelpers;
using Cassandra.Serialization;
using Cassandra.Tasks;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra
{
    internal abstract class SchemaParser : ISchemaParser
    {
        protected const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        private const int TraceMaxAttempts = 5;
        private const int TraceAttemptDelay = 400;
        private const string SelectTraceSessions = "SELECT * FROM system_traces.sessions WHERE session_id = {0}";
        private const string SelectTraceEvents = "SELECT * FROM system_traces.events WHERE session_id = {0}";
        
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
        public abstract Task<KeyspaceMetadata> GetKeyspaceAsync(string name);

        /// <summary>
        /// Gets all the keyspaces metadata
        /// </summary>
        public abstract Task<IEnumerable<KeyspaceMetadata>> GetKeyspacesAsync(bool retry);

        public abstract Task<TableMetadata> GetTableAsync(string keyspaceName, string tableName);

        public abstract Task<MaterializedViewMetadata> GetViewAsync(string keyspaceName, string viewName);

        public async Task<ICollection<string>> GetTableNamesAsync(string keyspaceName)
        {
            var rs = await Cc.QueryAsync(string.Format(SelectTables, keyspaceName), true).ConfigureAwait(false);
            return rs.Select(r => r.GetValue<string>(0)).ToArray();
        }

        public abstract Task<ICollection<string>> GetKeyspacesNamesAsync();

        public abstract Task<FunctionMetadata> GetFunctionAsync(string keyspaceName, string functionName, string signatureString);

        public abstract Task<AggregateMetadata> GetAggregateAsync(string keyspaceName, string aggregateName, string signatureString);

        public abstract Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspaceName, string typeName);

        public string ComputeFunctionSignatureString(string[] signature)
        {
            return "[" + string.Join(",", signature.Select(s => "'" + s + "'")) + "]";
        }

        public Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace, HashedWheelTimer timer)
        {
            return GetQueryTraceAsync(trace, timer, 0);
        }

        private Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace, HashedWheelTimer timer, int attempt)
        {
            if (attempt >= TraceMaxAttempts)
            {
                return TaskHelper.FromException<QueryTrace>(
                    new TraceRetrievalException(string.Format("Unable to retrieve complete query trace after {0} tries", TraceMaxAttempts)));
            }
            var sessionQuery = string.Format(SelectTraceSessions, trace.TraceId);
            var fetchAndAdapt = Cc
                .QueryAsync(sessionQuery)
                .ContinueSync(rs =>
                {
                    var sessionRow = rs.FirstOrDefault();
                    if (sessionRow == null || sessionRow.IsNull("duration") || sessionRow.IsNull("started_at"))
                    {
                        return null;
                    }
                    trace.RequestType = sessionRow.GetValue<string>("request");
                    trace.DurationMicros = sessionRow.GetValue<int>("duration");
                    trace.Coordinator = sessionRow.GetValue<IPAddress>("coordinator");
                    trace.Parameters = sessionRow.GetValue<IDictionary<string, string>>("parameters");
                    trace.StartedAt = sessionRow.GetValue<DateTimeOffset>("started_at").ToFileTime();
                    if (sessionRow.GetColumn("client") != null)
                    {
                        //client column was not present in previous
                        trace.ClientAddress = sessionRow.GetValue<IPAddress>("client");
                    }
                    return trace;
                });

            return fetchAndAdapt.Then(loadedTrace =>
            {
                if (loadedTrace == null)
                {
                    //Trace session was not loaded
                    return TaskHelper
                        .ScheduleExecution(() => GetQueryTraceAsync(trace, timer, attempt + 1), timer, TraceAttemptDelay)
                        .Unwrap();
                }
                var eventsQuery = string.Format(SelectTraceEvents, trace.TraceId);
                return Cc
                    .QueryAsync(eventsQuery)
                    .ContinueSync(rs =>
                    {
                        var events = rs
                            .Select(row => new QueryTrace.Event(
                                row.GetValue<string>("activity"),
                                row.GetValue<TimeUuid>("event_id").GetDate(),
                                row.GetValue<IPAddress>("source"),
                                row.GetValue<int?>("source_elapsed") ?? 0,
                                row.GetValue<string>("thread")))
                            .ToList();
                        loadedTrace.Events = events;
                        return loadedTrace;
                    });
            });
        }
    }

    /// <summary>
    /// Schema parser for metadata tables for Cassandra versions 2.2 or below
    /// </summary>
    internal class SchemaParserV1 : SchemaParser
    {
        private static readonly Task<TableMetadata> NullTableTask = TaskHelper.ToTask((TableMetadata)null);
        private const string SelectColumns = "SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectSingleTable = "SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspacesNames = "SELECT keyspace_name FROM system.schema_keyspaces";

        protected override string SelectAggregates => "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '{0}' AND aggregate_name = '{1}' AND signature = {2}";

        protected override string SelectFunctions => "SELECT * FROM system.schema_functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND signature = {2}";

        protected override string SelectTables => "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='{0}'";

        protected override string SelectUdts => "SELECT * FROM system.schema_usertypes WHERE keyspace_name='{0}' AND type_name = '{1}'";


        internal SchemaParserV1(Metadata parent) : base(parent)
        {

        }

        private KeyspaceMetadata ParseKeyspaceRow(IRow row)
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
                Utils.ConvertStringToMap(row.GetValue<string>("strategy_options")),
                new ReplicationStrategyFactory(),
                row.ContainsColumn("graph_engine") ? row.GetValue<string>("graph_engine") : null,
                false);
        }

        public override Task<KeyspaceMetadata> GetKeyspaceAsync(string name)
        {
            return Cc
                .QueryAsync(string.Format(SelectSingleKeyspace, name), true)
                .ContinueSync(rs => ParseKeyspaceRow(rs.FirstOrDefault()));
        }

        public override Task<IEnumerable<KeyspaceMetadata>> GetKeyspacesAsync(bool retry)
        {
            return Cc
                .QueryAsync(SelectKeyspaces, retry)
                .ContinueSync(rs => rs.Select(ParseKeyspaceRow));
        }
        
        public override async Task<ICollection<string>> GetKeyspacesNamesAsync()
        {
            var rs = await Cc.QueryAsync(SelectKeyspacesNames, true).ConfigureAwait(false);
            return rs.Select(r => r.GetValue<string>(0)).ToArray();
        }

        private static SortedDictionary<string, string> GetCompactionStrategyOptions(IRow row)
        {
            var result = new SortedDictionary<string, string> { { "class", row.GetValue<string>("compaction_strategy_class") } };
            foreach (var entry in Utils.ConvertStringToMap(row.GetValue<string>("compaction_strategy_options")))
            {
                result.Add(entry.Key, entry.Value);
            }
            return result;
        }

        public override Task<TableMetadata> GetTableAsync(string keyspaceName, string tableName)
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
                            (SortedDictionary<string, string>)Utils.ConvertStringToMap(tableMetadataRow.GetValue<string>("compression_parameters"))
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
                                var dataType = DataTypeParser.ParseFqTypeName(row.GetValue<string>("validator"));
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
                                            : KeyType.None
                                };
                                if (row.GetColumn("type") != null)
                                {
                                    switch (row.GetValue<string>("type"))
                                    {
                                        case "partition_key":
                                            partitionKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, col));
                                            col.KeyType = KeyType.Partition;
                                            break;
                                        case "clustering_key":
                                            var sortOrder = dataType.IsReversed ? SortOrder.Descending : SortOrder.Ascending;
                                            clusteringKeys.Add(Tuple.Create(row.GetValue<int?>("component_index") ?? 0, Tuple.Create(col, sortOrder)));
                                            col.KeyType = KeyType.Clustering;
                                            break;
                                        case "static":
                                            col.IsStatic = true;
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
                                    if (!columns.TryGetValue(name, out TableColumn c))
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
                                        var dataType = types[i];
                                        if (!columns.TryGetValue(name, out TableColumn c))
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
                                options.isCompactStorage = !comparator.StartsWith(DataTypeParser.CompositeTypeName);
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
                types[i] = DataTypeParser.ParseFqTypeName(typesString, indexes[i], indexes[i + 1] - indexes[i] - 1);
            }
            return types;
        }

        public override Task<MaterializedViewMetadata> GetViewAsync(string keyspaceName, string viewName)
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

        public override Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspaceName, string typeName)
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
                        var field = DataTypeParser.ParseFqTypeName(fieldTypes[i]);
                        field.Name = fieldNames[i];
                        udt.Fields.Add(field);
                    }
                    return udt;
                });
        }

        public override Task<FunctionMetadata> GetFunctionAsync(string keyspaceName, string functionName, string signatureString)
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
                        ReturnType = DataTypeParser.ParseFqTypeName(row.GetValue<string>("return_type")),
                        ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => DataTypeParser.ParseFqTypeName(s)).ToArray()
                    };
                });
        }

        public override Task<AggregateMetadata> GetAggregateAsync(string keyspaceName, string aggregateName, string signatureString)
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
                        StateType = DataTypeParser.ParseFqTypeName(row.GetValue<string>("state_type")),
                        FinalFunction = row.GetValue<string>("final_func"),
                        ReturnType = DataTypeParser.ParseFqTypeName(row.GetValue<string>("return_type")),
                        ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => DataTypeParser.ParseFqTypeName(s)).ToArray(),
                    };
                    var initConditionRaw = Deserialize(Cc, row.GetValue<byte[]>("initcond"), aggregate.StateType.TypeCode, aggregate.StateType.TypeInfo);
                    if (initConditionRaw != null)
                    {
                        aggregate.InitialCondition = initConditionRaw.ToString();
                    }
                    return aggregate;
                });
        }

        private static object Deserialize(IMetadataQueryProvider cc, byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (buffer == null)
            {
                return null;
            }
            return cc.Serializer.GetCurrentSerializer().Deserialize(buffer, 0, buffer.Length, typeCode, typeInfo);
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
        protected const string SelectKeyspaces = "SELECT * FROM system_schema.keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectSingleTable = "SELECT * FROM system_schema.tables WHERE table_name='{0}' AND keyspace_name='{1}'";
        private const string SelectSingleView = "SELECT * FROM system_schema.views WHERE view_name='{0}' AND keyspace_name='{1}'";
        private const string SelectKeyspacesNames = "SELECT keyspace_name FROM system_schema.keyspaces";

        protected override string SelectAggregates => "SELECT * FROM system_schema.aggregates WHERE keyspace_name = '{0}' AND aggregate_name = '{1}' AND argument_types = {2}";

        protected override string SelectFunctions => "SELECT * FROM system_schema.functions WHERE keyspace_name = '{0}' AND function_name = '{1}' AND argument_types = {2}";

        protected override string SelectTables => "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{0}'";

        protected override string SelectUdts => "SELECT * FROM system_schema.types WHERE keyspace_name='{0}' AND type_name = '{1}'";
        
        internal SchemaParserV2(Metadata parent, Func<string, string, Task<UdtColumnInfo>> udtResolver)
            : base(parent)
        {
            _udtResolver = udtResolver;
        }

        private KeyspaceMetadata ParseKeyspaceRow(IRow row)
        {
            var replication = row.GetValue<IDictionary<string, string>>("replication");
            string strategy = null;

            if (replication != null)
            {
                strategy = replication["class"];
            }

            replication?.Remove("class");

            return new KeyspaceMetadata(
                Parent,
                row.GetValue<string>("keyspace_name"),
                row.GetValue<bool>("durable_writes"),
                strategy,
                replication,
                new ReplicationStrategyFactory(),
                row.ContainsColumn("graph_engine") ? row.GetValue<string>("graph_engine") : null);
        }

        public override async Task<KeyspaceMetadata> GetKeyspaceAsync(string name)
        {
            var rs = await Cc.QueryAsync(string.Format(SelectSingleKeyspace, name), true).ConfigureAwait(false);
            var row = rs.FirstOrDefault();
            return row != null ? ParseKeyspaceRow(row) : null;
        }

        public override async Task<IEnumerable<KeyspaceMetadata>> GetKeyspacesAsync(bool retry)
        {
            var rs = await Cc.QueryAsync(SelectKeyspaces, retry).ConfigureAwait(false);
            return rs.Select(ParseKeyspaceRow);
        }
        
        public override async Task<ICollection<string>> GetKeyspacesNamesAsync()
        {
            var rs = await Cc.QueryAsync(SelectKeyspacesNames, true).ConfigureAwait(false);
            return rs.Select(r => r.GetValue<string>(0)).ToArray();
        }

        public override async Task<TableMetadata> GetTableAsync(string keyspaceName, string tableName)
        {
            var getTableTask = Cc.QueryAsync(string.Format(SelectSingleTable, tableName, keyspaceName), true);
            var getColumnsTask = Cc.QueryAsync(string.Format(SelectColumns, tableName, keyspaceName), true);
            var getIndexesTask = Cc.QueryAsync(string.Format(SelectIndexes, tableName, keyspaceName), true);

            await Task.WhenAll(getTableTask, getColumnsTask, getIndexesTask).ConfigureAwait(false);

            var indexesRs = await getIndexesTask.ConfigureAwait(false);
            var tableRs = await getTableTask.ConfigureAwait(false);
            var columnsRs = await getColumnsTask.ConfigureAwait(false);

            var indexes = GetIndexes(indexesRs);

            return await ParseTableOrView(_ => new TableMetadata(tableName, indexes), tableRs, columnsRs)
                .ConfigureAwait(false);
        }

        protected async Task<T> ParseTableOrView<T>(Func<IRow, T> newInstance, IEnumerable<IRow> tableRs,
                                                    IEnumerable<IRow> columnsRs) where T : DataCollectionMetadata
        {
            var tableMetadataRow = tableRs.FirstOrDefault();
            if (tableMetadataRow == null)
            {
                return null;
            }

            var columns = new Dictionary<string, TableColumn>();
            var partitionKeys = new List<Tuple<int, TableColumn>>();
            var clusteringKeys = new List<Tuple<int, Tuple<TableColumn, SortOrder>>>();
            TableOptions options;

            if (tableMetadataRow.ContainsColumn("compression"))
            {
                // Options for normal tables and views
                options = new TableOptions
                {
                    isCompactStorage = false,
                    bfFpChance = tableMetadataRow.GetValue<double>("bloom_filter_fp_chance"),
                    caching = "{" + string.Join(",", tableMetadataRow.GetValue<IDictionary<string, string>>("caching")
                                                                     .Select(kv => "\"" + kv.Key + "\":\"" + kv.Value + "\"")) + "}",
                    comment = tableMetadataRow.GetValue<string>("comment"),
                    gcGrace = tableMetadataRow.GetValue<int>("gc_grace_seconds"),
                    localReadRepair = tableMetadataRow.GetValue<double>("dclocal_read_repair_chance"),
                    readRepair = tableMetadataRow.GetValue<double>("read_repair_chance"),
                    compactionOptions = tableMetadataRow.GetValue<SortedDictionary<string, string>>("compaction"),
                    compressionParams =
                        tableMetadataRow.GetValue<SortedDictionary<string, string>>("compression")
                };
            }
            else
            {
                // Options for virtual tables
                options = new TableOptions { comment = tableMetadataRow.GetValue<string>("comment") };
            }

            options.NodeSync = tableMetadataRow.GetColumn("nodesync") != null
                ? tableMetadataRow.GetValue<IDictionary<string, string>>("nodesync")
                : null;

            var columnTasks = columnsRs
                .Select(async row =>
                {
                    var type = await DataTypeParser
                        .ParseTypeName(
                             _udtResolver, tableMetadataRow.GetValue<string>("keyspace_name"),
                             row.GetValue<string>("type"))
                        .ConfigureAwait(false);

                    return Tuple.Create(new TableColumn
                    {
                        Name = row.GetValue<string>("column_name"),
                        Keyspace = row.GetValue<string>("keyspace_name"),
                        Table = row.GetValue<string>("table_name"),
                        TypeCode = type.TypeCode,
                        TypeInfo = type.TypeInfo
                    }, row);
                });

            var columnsTuples = await Task.WhenAll(columnTasks).ConfigureAwait(false);

            foreach (var t in columnsTuples)
            {
                var col = t.Item1;
                var row = t.Item2;

                switch (row.GetValue<string>("kind"))
                {
                    case "partition_key":
                        partitionKeys.Add(Tuple.Create(row.GetValue<int?>("position") ?? 0, col));
                        col.KeyType = KeyType.Partition;
                        break;
                    case "clustering":
                        clusteringKeys.Add(Tuple.Create(row.GetValue<int?>("position") ?? 0,
                            Tuple.Create(col, row.GetValue<string>("clustering_order") == "desc" ? SortOrder.Descending : SortOrder.Ascending)));
                        col.KeyType = KeyType.Clustering;
                        break;
                    case "static":
                        col.IsStatic = true;
                        break;
                }
                columns.Add(col.Name, col);
            }

            if (tableMetadataRow.ContainsColumn("flags"))
            {
                // Normal tables
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
        }

        public override async Task<MaterializedViewMetadata> GetViewAsync(string keyspaceName, string viewName)
        {
            var getTableTask = Cc.QueryAsync(string.Format(SelectSingleView, viewName, keyspaceName), true);
            var getColumnsTask = Cc.QueryAsync(string.Format(SelectColumns, viewName, keyspaceName), true);

            var tableRs = await getTableTask.ConfigureAwait(false);
            var columnsRs = await getColumnsTask.ConfigureAwait(false);

            return await ParseTableOrView(
                viewRow => new MaterializedViewMetadata(viewName, viewRow.GetValue<string>("where_clause")),
                tableRs,
                columnsRs).ConfigureAwait(false);
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
            if (columns.TryGetValue("value", out TableColumn valueBlob) && valueBlob.TypeCode == ColumnTypeCode.Blob)
            {
                columns.Remove("value");
            }
        }

        private static IDictionary<string, IndexMetadata> GetIndexes(IEnumerable<IRow> rows)
        {
            return rows.Select(IndexMetadata.FromRow).ToDictionary(ix => ix.Name);
        }

        public override Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspaceName, string typeName)
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
                        .Select(name => DataTypeParser.ParseTypeName(_udtResolver, keyspaceName, name))
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

        public override Task<AggregateMetadata> GetAggregateAsync(string keyspaceName, string aggregateName, string signatureString)
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
                    parseTasks[0] = DataTypeParser.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("state_type"));
                    parseTasks[1] = DataTypeParser.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("return_type"));
                    for (var i = 0; i < argumentTypes.Length; i++)
                    {
                        parseTasks[2 + i] = DataTypeParser.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), argumentTypes[i]);
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
                            Deterministic = row.GetColumn("deterministic") != null &&
                                            row.GetValue<bool>("deterministic"),
                            Signature = argumentTypes,
                            StateType = tasks[0].Result,
                            ReturnType = tasks[1].Result,
                            ArgumentTypes = tasks.Skip(2).Select(t => t.Result).ToArray()
                        };
                    }, TaskContinuationOptions.ExecuteSynchronously);
                });
        }

        public override Task<FunctionMetadata> GetFunctionAsync(string keyspaceName, string functionName, string signatureString)
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
                    parseTasks[0] = DataTypeParser.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), row.GetValue<string>("return_type"));
                    for (var i = 0; i < argumentTypes.Length; i++)
                    {
                        parseTasks[1 + i] = DataTypeParser.ParseTypeName(_udtResolver, row.GetValue<string>("keyspace_name"), argumentTypes[i]);
                    }
                    return Task.Factory.ContinueWhenAll(parseTasks, tasks =>
                    {
                        var ex = tasks.Select(t => t.Exception).FirstOrDefault(e => e != null);
                        if (ex != null)
                        {
                            throw ex.InnerException;
                        }

                        var result = new FunctionMetadata
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

                        if (row.GetColumn("deterministic") != null)
                        {
                            // DSE 6.0+
                            result.Deterministic = row.GetValue<bool>("deterministic");
                            result.Monotonic = row.GetValue<bool>("monotonic");
                            result.MonotonicOn = row.GetValue<string[]>("monotonic_on");
                        }

                        return result;
                    });
                });
        }
    }

    /// <summary>
    /// Schema parser for Apache Cassandra version 4.x and above
    /// </summary>
    internal class SchemaParserV3 : SchemaParserV2
    {
        private const string SelectVirtualKeyspaces = "SELECT * FROM system_virtual_schema.keyspaces";
        private const string SelectSingleVirtualKeyspace =
            "SELECT * FROM system_virtual_schema.keyspaces WHERE keyspace_name = '{0}'";
        private const string SelectVirtualTable =
            "SELECT * FROM system_virtual_schema.tables WHERE keyspace_name = '{0}' AND table_name='{1}'";
        private const string SelectVirtualColumns =
            "SELECT * FROM system_virtual_schema.columns WHERE keyspace_name = '{0}' AND table_name='{1}'";
        private const string SelectVirtualKeyspaceNames = "SELECT keyspace_name FROM system_virtual_schema.keyspaces";

        internal SchemaParserV3(Metadata parent, Func<string, string, Task<UdtColumnInfo>> udtResolver)
            : base(parent, udtResolver)
        {

        }

        public override async Task<KeyspaceMetadata> GetKeyspaceAsync(string name)
        {
            var ks = await base.GetKeyspaceAsync(name).ConfigureAwait(false);
            if (ks != null)
            {
                return ks;
            }

            // Maybe its a virtual keyspace
            IEnumerable<IRow> rs;
            try
            {
                rs = await Cc.QueryAsync(string.Format(SelectSingleVirtualKeyspace, name), true)
                             .ConfigureAwait(false);
            }
            catch (InvalidQueryException)
            {
                // Incorrect version reported by the server: virtual keyspaces/tables are not yet supported
                return null;
            }

            var row = rs.FirstOrDefault();
            return row != null ? ParseVirtualKeyspaceRow(row) : null;
        }

        private KeyspaceMetadata ParseVirtualKeyspaceRow(IRow row)
        {
            return new KeyspaceMetadata(
                Parent,
                row.GetValue<string>("keyspace_name"),
                true,
                null,
                null,
                new ReplicationStrategyFactory(),
                row.ContainsColumn("graph_engine") ? row.GetValue<string>("graph_engine") : null,
                true);
        }

        public override async Task<IEnumerable<KeyspaceMetadata>> GetKeyspacesAsync(bool retry)
        {
            // Start the task to get the keyspaces in parallel
            var keyspacesTask = base.GetKeyspacesAsync(retry);
            var virtualKeyspaces = Enumerable.Empty<KeyspaceMetadata>();

            try
            {
                var rs = await Cc.QueryAsync(SchemaParserV3.SelectVirtualKeyspaces, retry).ConfigureAwait(false);
                virtualKeyspaces = rs.Select(ParseVirtualKeyspaceRow);
            }
            catch (InvalidQueryException)
            {
                // Incorrect version reported by the server: virtual keyspaces/tables are not yet supported
            }

            var keyspaces = await keyspacesTask.ConfigureAwait(false);

            // Yield the keyspaces followed by the virtual keyspaces
            return keyspaces.Concat(virtualKeyspaces);
        }
        
        public override async Task<ICollection<string>> GetKeyspacesNamesAsync()
        {
            // Start the task to get the keyspace names in parallel
            var keyspacesTask = base.GetKeyspacesNamesAsync();
            var virtualKeyspaces = Enumerable.Empty<string>();

            try
            {
                var rs = await Cc.QueryAsync(SelectVirtualKeyspaceNames, true).ConfigureAwait(false);
                virtualKeyspaces = rs.Select(r => r.GetValue<string>(0));
            }
            catch (InvalidQueryException)
            {
                // Incorrect version reported by the server: virtual keyspaces/tables are not yet supported
            }

            var keyspaces = await keyspacesTask.ConfigureAwait(false);

            // Yield the keyspaces followed by the virtual keyspaces
            return keyspaces.Concat(virtualKeyspaces).ToArray();
        }

        public override async Task<TableMetadata> GetTableAsync(string keyspaceName, string tableName)
        {
            var table = await base.GetTableAsync(keyspaceName, tableName).ConfigureAwait(false);
            if (table != null)
            {
                return table;
            }

            IEnumerable<IRow> tableRs;
            try
            {
                // Maybe its a virtual table
                tableRs = await Cc.QueryAsync(string.Format(SelectVirtualTable, keyspaceName, tableName), true)
                                  .ConfigureAwait(false);
            }
            catch (InvalidQueryException)
            {
                // Incorrect version reported by the server: virtual keyspaces/tables are not yet supported
                return null;
            }

            var columnsRs = await Cc.QueryAsync(string.Format(SelectVirtualColumns, keyspaceName, tableName), true)
                                    .ConfigureAwait(false);

            return await ParseTableOrView(_ => new TableMetadata(tableName, null, true), tableRs, columnsRs)
                .ConfigureAwait(false);
        }
    }
}
