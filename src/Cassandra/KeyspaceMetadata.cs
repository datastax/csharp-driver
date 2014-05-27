//
//      Copyright (C) 2012 DataStax Inc.
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public class KeyspaceMetadata
    {
        internal readonly AtomicValue<ConcurrentDictionary<string, AtomicValue<TableMetadata>>> Tables =
            new AtomicValue<ConcurrentDictionary<string, AtomicValue<TableMetadata>>>(null);

        private readonly ControlConnection _cc;

        /// <summary>
        ///  Gets the name of this keyspace.
        /// </summary>
        /// 
        /// <returns>the name of this CQL keyspace.</returns>
        public string Name { get; private set; }

        /// <summary>
        ///  Gets a value indicating whether durable writes are set on this keyspace.
        /// </summary>
        /// 
        /// <returns><c>true</c> if durable writes are set on this keyspace
        ///  , <c>false</c> otherwise.</returns>
        public bool DurableWrites { get; private set; }

        /// <summary>
        ///  Gets the Strategy Class of this keyspace.
        /// </summary>
        /// 
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
            Replication = replicationOptions;
        }


        /// <summary>
        ///  Returns metadata of specified table in this keyspace.
        /// </summary>
        /// <param name="tableName"> the name of table to retrieve </param>
        /// 
        /// <returns>the metadata for table <c>tableName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public TableMetadata GetTableMetadata(string tableName)
        {
            return _cc.GetTable(Name, tableName);
        }


        /// <summary>
        ///  Returns metadata of all tables defined in this keyspace.
        /// </summary>
        /// 
        /// <returns>an IEnumerable of TableMetadata for the tables defined in this
        ///  keyspace.</returns>
        public IEnumerable<TableMetadata> GetTablesMetadata()
        {
            foreach (string tableName in _cc.GetTables(Name))
                yield return _cc.GetTable(Name, tableName);
        }


        /// <summary>
        ///  Returns names of all tables defined in this keyspace.
        /// </summary>
        /// 
        /// <returns>a collection of all, defined in this
        ///  keyspace tables names.</returns>
        public ICollection<string> GetTablesNames()
        {
            return _cc.GetTables(Name);
        }

        /// <summary>
        ///  Return a <c>String</c> containing CQL queries representing this
        ///  name and the table it contains. In other words, this method returns the
        ///  queries that would allow to recreate the schema of this name, along with
        ///  all its table. Note that the returned String is formatted to be human
        ///  readable (for some defintion of human readable at least).
        /// </summary>
        /// 
        /// <returns>the CQL queries representing this name schema as a code
        ///  String}.</returns>
        public string ExportAsString()
        {
            var sb = new StringBuilder();

            sb.Append(AsCqlQuery()).Append("\n");

            //foreach (var tm in Tables.Value.Values)
            //    sb.Append("\n").Append(tm.Value.exportAsString()).Append("\n");

            return sb.ToString();
        }


        /// <summary>
        ///  Returns a CQL query representing this keyspace. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </summary>
        /// 
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.
        ///  <see>#ExportAsString</see></returns>
        public string AsCqlQuery()
        {
            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.QuoteIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(Replication["class"]).Append("'");
            foreach (KeyValuePair<string, int> rep in Replication)
            {
                if (rep.Key == "class")
                    continue;
                sb.Append(", '").Append(rep.Key).Append("': '").Append(rep.Value).Append("'");
            }
            sb.Append(" } AND DURABLE_WRITES = ").Append(DurableWrites);
            sb.Append(";");
            return sb.ToString();
        }
    }
}