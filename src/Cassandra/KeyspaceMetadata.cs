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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.MetadataHelpers;
using Cassandra.Tasks;

namespace Cassandra
{
    public class KeyspaceMetadata
    {
        private readonly ConcurrentDictionary<string, TableMetadata> _tables = new ConcurrentDictionary<string, TableMetadata>();
        private readonly ConcurrentDictionary<string, MaterializedViewMetadata> _views = new ConcurrentDictionary<string, MaterializedViewMetadata>();
        private readonly ConcurrentDictionary<Tuple<string, string>, FunctionMetadata> _functions = new ConcurrentDictionary<Tuple<string, string>, FunctionMetadata>();
        private readonly ConcurrentDictionary<Tuple<string, string>, AggregateMetadata> _aggregates = new ConcurrentDictionary<Tuple<string, string>, AggregateMetadata>();
        private readonly Metadata _parent;

        /// <summary>
        ///  Gets the name of this keyspace.
        /// </summary>
        /// <returns>the name of this CQL keyspace.</returns>
        public string Name { get; }

        /// <summary>
        ///  Gets a value indicating whether durable writes are set on this keyspace.
        /// </summary>
        /// <returns><c>true</c> if durable writes are set on this keyspace
        ///  , <c>false</c> otherwise.</returns>
        public bool DurableWrites { get; }

        /// <summary>
        ///  Gets the Strategy Class of this keyspace.
        /// </summary>
        /// <returns>name of StrategyClass of this keyspace.</returns>
        public string StrategyClass { get; }

        /// <summary>
        ///  Returns the replication options for this keyspace.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the keyspace replication strategy options.</returns>
        public IDictionary<string, int> Replication { get; }

        /// <summary>
        ///  Returns the replication options for this keyspace.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the keyspace replication strategy options.</returns>
        private IDictionary<string, string> ReplicationOptions { get; }

        /// <summary>
        /// Determines whether the keyspace is a virtual keyspace or not.
        /// </summary>
        public bool IsVirtual { get; }
        
        /// <summary>
        /// Returns the graph engine associated with this keyspace. Returns null if there isn't one.
        /// </summary>
        public string GraphEngine { get; }

        internal IReplicationStrategy Strategy { get; }
        
        internal KeyspaceMetadata(
            Metadata parent, 
            string name, 
            bool durableWrites, 
            string strategyClass,
            IDictionary<string, string> replicationOptions,
            IReplicationStrategyFactory replicationStrategyFactory,
            string graphEngine,
            bool isVirtual = false)
        {
            //Can not directly reference to schemaParser as it might change
            _parent = parent;
            Name = name;
            DurableWrites = durableWrites;

            if (strategyClass != null && strategyClass.StartsWith("org.apache.cassandra.locator."))
            {
                strategyClass = strategyClass.Replace("org.apache.cassandra.locator.", "");
            }

            StrategyClass = strategyClass;

            var parsedReplicationOptions = replicationOptions == null
                ? null 
                : ParseReplicationFactors(replicationOptions);

            Replication = parsedReplicationOptions == null 
                ? null 
                : ConvertReplicationOptionsToLegacy(parsedReplicationOptions);
            
            ReplicationOptions = replicationOptions;
            IsVirtual = isVirtual;
            Strategy = 
                (strategyClass == null || parsedReplicationOptions == null) 
                ? null 
                : replicationStrategyFactory.Create(StrategyClass, parsedReplicationOptions);

            GraphEngine = graphEngine;
        }

        /// <summary>
        ///  Returns metadata of specified table in this keyspace.
        /// </summary>
        /// <param name="tableName"> the name of table to retrieve </param>
        /// <returns>the metadata for table <c>tableName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public TableMetadata GetTableMetadata(string tableName)
        {
            return TaskHelper.WaitToComplete(
                GetTableMetadataAsync(tableName), _parent.Configuration.DefaultRequestOptions.GetQueryAbortTimeout(2));
        }

        internal async Task<TableMetadata> GetTableMetadataAsync(string tableName)
        {
            if (_tables.TryGetValue(tableName, out var tableMetadata))
            {
                //The table metadata is available in local cache
                return tableMetadata;
            }

            var table = await _parent.SchemaParser.GetTableAsync(Name, tableName).ConfigureAwait(false);
            
            if (table == null)
            {
                return null;
            }

            //Cache it
            _tables.AddOrUpdate(tableName, table, (k, o) => table);
            return table;
        }

        /// <summary>
        ///  Returns metadata of specified view in this keyspace.
        /// </summary>
        /// <param name="viewName">the name of view to retrieve </param>
        /// <returns>the metadata for view <c>viewName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public MaterializedViewMetadata GetMaterializedViewMetadata(string viewName)
        {
            return TaskHelper.WaitToComplete(
                GetMaterializedViewMetadataAsync(viewName), _parent.Configuration.DefaultRequestOptions.GetQueryAbortTimeout(2));
        }

        private async Task<MaterializedViewMetadata> GetMaterializedViewMetadataAsync(string viewName)
        {
            if (_views.TryGetValue(viewName, out var v))
            {
                //The table metadata is available in local cache
                return v;
            }

            var view = await _parent.SchemaParser.GetViewAsync(Name, viewName).ConfigureAwait(false);
            if (view == null)
            {
                return null;
            }

            //Cache it
            _views.AddOrUpdate(viewName, view, (k, o) => view);
            return view;
        }

        /// <summary>
        /// Removes table metadata forcing refresh the next time the table metadata is retrieved
        /// </summary>
        internal void ClearTableMetadata(string tableName)
        {
            _tables.TryRemove(tableName, out _);
        }

        /// <summary>
        /// Removes the view metadata forcing refresh the next time the view metadata is retrieved
        /// </summary>
        internal void ClearViewMetadata(string name)
        {
            _views.TryRemove(name, out _);
        }

        /// <summary>
        /// Removes function metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearFunction(string name, string[] signature)
        {
            _functions.TryRemove(KeyspaceMetadata.GetFunctionKey(name, signature), out _);
        }

        /// <summary>
        /// Removes aggregate metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearAggregate(string name, string[] signature)
        {
            _aggregates.TryRemove(KeyspaceMetadata.GetFunctionKey(name, signature), out _);
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
            return TaskHelper.WaitToComplete(_parent.SchemaParser.GetTableNamesAsync(Name));
        }
        
        /// <summary>
        /// <para>
        ///  Deprecated. Please use <see cref="AsCqlQuery"/>.
        /// </para>
        /// <para>
        ///  Returns a CQL query representing this keyspace. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </para>
        /// </summary>
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.</returns>
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
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.</returns>
        public string AsCqlQuery()
        {
            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.QuoteIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(StrategyClass).Append("'");
            foreach (var rep in ReplicationOptions)
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
            return TaskHelper.WaitToComplete(GetUdtDefinitionAsync(typeName), _parent.Configuration.DefaultRequestOptions.QueryAbortTimeout);
        }

        /// <summary>
        /// Gets the definition of a User defined type asynchronously
        /// </summary>
        internal Task<UdtColumnInfo> GetUdtDefinitionAsync(string typeName)
        {
            return _parent.SchemaParser.GetUdtDefinitionAsync(Name, typeName);
        }

        /// <summary>
        /// Gets a CQL function by name and signature
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string functionName, string[] signature)
        {
            return TaskHelper.WaitToComplete(
                GetFunctionAsync(functionName, signature), _parent.Configuration.DefaultRequestOptions.QueryAbortTimeout);
        }

        private async Task<FunctionMetadata> GetFunctionAsync(string functionName, string[] signature)
        {
            if (signature == null)
            {
                signature = new string[0];
            }

            var key = KeyspaceMetadata.GetFunctionKey(functionName, signature);
            if (_functions.TryGetValue(key, out var func))
            {
                return func;
            }

            var signatureString = _parent.SchemaParser.ComputeFunctionSignatureString(signature);
            var f = await _parent.SchemaParser.GetFunctionAsync(Name, functionName, signatureString).ConfigureAwait(false);
            if (f == null)
            {
                return null;
            }

            _functions.AddOrUpdate(key, f, (k, v) => f);
            return f;
        }

        /// <summary>
        /// Gets a CQL aggregate by name and signature
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string aggregateName, string[] signature)
        {
            return TaskHelper.WaitToComplete(
                GetAggregateAsync(aggregateName, signature), _parent.Configuration.DefaultRequestOptions.QueryAbortTimeout);
        }

        private async Task<AggregateMetadata> GetAggregateAsync(string aggregateName, string[] signature)
        {
            if (signature == null)
            {
                signature = new string[0];
            }

            var key = KeyspaceMetadata.GetFunctionKey(aggregateName, signature);
            if (_aggregates.TryGetValue(key, out var aggregate))
            {
                return aggregate;
            }

            var signatureString = _parent.SchemaParser.ComputeFunctionSignatureString(signature);
            var a = await _parent.SchemaParser.GetAggregateAsync(Name, aggregateName, signatureString).ConfigureAwait(false);
            if (a == null)
            {
                return null;
            }

            _aggregates.AddOrUpdate(key, a, (k, v) => a);
            return a;
        }

        private static Tuple<string, string> GetFunctionKey(string name, string[] signature)
        {
            return Tuple.Create(name, string.Join(",", signature));
        }

        /// <summary>
        /// This is needed in order to avoid breaking the public API (see <see cref="Replication"/>
        /// </summary>
        private IDictionary<string, int> ConvertReplicationOptionsToLegacy(IDictionary<string, ReplicationFactor> replicationOptions)
        {
            return replicationOptions.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.AllReplicas);
        }

        private Dictionary<string, ReplicationFactor> ParseReplicationFactors(IDictionary<string, string> replicationOptions)
        {
            return replicationOptions.ToDictionary(kvp => kvp.Key, kvp => ReplicationFactor.Parse(kvp.Value));
        }
    }
}
