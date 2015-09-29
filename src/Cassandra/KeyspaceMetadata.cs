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
using Cassandra.Tasks;

namespace Cassandra
{
    public class KeyspaceMetadata
    {
        private readonly ConcurrentDictionary<string, TableMetadata> _tables = new ConcurrentDictionary<string, TableMetadata>();
        private readonly ConcurrentDictionary<Tuple<string, string>, FunctionMetadata> _functions = new ConcurrentDictionary<Tuple<string, string>, FunctionMetadata>();
        private readonly ConcurrentDictionary<Tuple<string, string>, AggregateMetadata> _aggregates = new ConcurrentDictionary<Tuple<string, string>, AggregateMetadata>();
        private readonly SchemaParser _parser;
        private readonly IMetadataQueryProvider _queryProvider;

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

        internal KeyspaceMetadata(SchemaParser parser, IMetadataQueryProvider queryProvider, string name, bool durableWrites, string strategyClass,
                                  IDictionary<string, int> replicationOptions)
        {
            _parser = parser;
            _queryProvider = queryProvider;
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
            var task = _parser
                .GetTable(_queryProvider, Name, tableName)
                .ContinueSync(t =>
                {
                    if (t == null)
                    {
                        return null;
                    }
                    //Cache it
                    _tables.AddOrUpdate(tableName, t, (k, o) => t);
                    return t;
                });
            return TaskHelper.WaitToComplete(task, ControlConnection.MetadataAbortTimeout);
        }

        /// <summary>
        /// Removes table metadata forcing refresh the next time the table metadata is retrieved
        /// </summary>
        internal void ClearTableMetadata(string tableName)
        {
            TableMetadata table;
            _tables.TryRemove(tableName, out table);
        }

        /// <summary>
        /// Removes function metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearFunction(string name, string[] signature)
        {
            FunctionMetadata element;
            _functions.TryRemove(GetFunctionKey(name, signature), out element);
        }

        /// <summary>
        /// Removes aggregate metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearAggregate(string name, string[] signature)
        {
            AggregateMetadata element;
            _aggregates.TryRemove(GetFunctionKey(name, signature), out element);
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
            return TaskHelper.WaitToComplete(_parser.GetTableNames(_queryProvider, Name));
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
            return TaskHelper.WaitToComplete(_parser.GetUdtDefinition(_queryProvider, Name, typeName), ControlConnection.MetadataAbortTimeout);
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
            var key = GetFunctionKey(functionName, signature);
            if (_functions.TryGetValue(key, out func))
            {
                return func;
            }
            var signatureString = "[" + string.Join(",", signature.Select(s => "'" + s + "'")) + "]";
            var t = _parser
                .GetFunction(_queryProvider, Name, functionName, signatureString)
                .ContinueSync(f =>
                {
                    if (f == null)
                    {
                        return null;
                    }
                    _functions.AddOrUpdate(key, func, (k, v) => func);
                    return f;
                });
            return TaskHelper.WaitToComplete(t, ControlConnection.MetadataAbortTimeout);
        }

        /// <summary>
        /// Gets a CQL aggregate by name and signature
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string aggregateName, string[] signature)
        {
            if (signature == null)
            {
                signature = new string[0];
            }
            AggregateMetadata aggregate;
            var key = GetFunctionKey(aggregateName, signature);
            if (_aggregates.TryGetValue(key, out aggregate))
            {
                return aggregate;
            }
            var signatureString = "[" + string.Join(",", signature.Select(s => "'" + s + "'")) + "]";
            var t = _parser
                .GetAggregate(_queryProvider, Name, aggregateName, signatureString)
                .ContinueSync(a =>
                {
                    if (a == null)
                    {
                        return null;
                    }
                    _aggregates.AddOrUpdate(key, a, (k, v) => a);
                    return a;
                });
            return TaskHelper.WaitToComplete(t, ControlConnection.MetadataAbortTimeout);
        }

        private static Tuple<string, string> GetFunctionKey(string name, string[] signature)
        {
            return Tuple.Create(name, String.Join(",", signature));
        }
    }
}
