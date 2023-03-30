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
using System.Reflection;
using Cassandra.Mapping.Utils;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Represents data about a POCO and its mapping to Cassandra Rows in a table.
    /// </summary>
    internal class PocoData : IPocoData
    {
        private readonly Dictionary<string, PocoColumn> _columnsByMemberName;
        private readonly HashSet<string> _primaryKeys;
        /// <summary>
        /// The .NET Type of the POCO this data is for.
        /// </summary>
        public Type PocoType { get; private set; }

        /// <summary>
        /// The table name the POCO maps to in C*.
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// When defined, states that all queries generated should include fully qualified table names (ie: keyspace.table)
        /// </summary>
        public string KeyspaceName { get; set; }

        /// <summary>
        /// All columns (including PK columns) keyed by their column names and ordered so that the primary key columns are in order last.
        /// </summary>
        public LookupKeyedCollection<string, PocoColumn> Columns { get; private set; }

        /// <summary>
        /// Gets the partition key columns of the table.
        /// </summary>
        public List<PocoColumn> PartitionKeys { get; set; }

        /// <summary>
        /// Gets the clustering key columns of the table.
        /// </summary>
        public List<Tuple<PocoColumn, SortOrder>> ClusteringKeys { get; private set; }

        /// <summary>
        /// Determines if the queries generated using this poco information should be case-sensitive
        /// </summary>
        public bool CaseSensitive { get; private set; }

        public bool CompactStorage { get; private set; }

        /// <summary>
        /// Determines that all queries generated should allow filtering. Backwards compatibility.
        /// </summary>
        public bool AllowFiltering { get; private set; }

        /// <summary>
        /// The column names of any primary key columns that aren't in the Columns collection.  Could indicate a misconfiguration if the POCO
        /// is going to be used in auto-generated UPDATE/DELETE statements.
        /// </summary>
        public List<string> MissingPrimaryKeyColumns { get; private set; }

        public PocoData(Type pocoType, string tableName, string keyspaceName, LookupKeyedCollection<string, PocoColumn> columns,
                        string[] partitionkeys, Tuple<string, SortOrder>[] clusteringKeys, bool caseSensitive, bool compact, bool allowFiltering)
        {
            if (partitionkeys == null) throw new ArgumentNullException("partitionkeys");
            if (clusteringKeys == null) throw new ArgumentNullException("clusteringKeys");
            PocoType = pocoType ?? throw new ArgumentNullException("pocoType");
            TableName = tableName ?? throw new ArgumentNullException("tableName");
            Columns = columns ?? throw new ArgumentNullException("columns");
            CaseSensitive = caseSensitive;
            CompactStorage = compact;
            AllowFiltering = allowFiltering;
            KeyspaceName = keyspaceName;
            _columnsByMemberName = columns.ToDictionary(c => c.MemberInfo.Name, c => c);
            PartitionKeys = partitionkeys.Where(columns.Contains).Select(key => columns[key]).ToList();
            ClusteringKeys = clusteringKeys.Where(c => columns.Contains(c.Item1)).Select(c => Tuple.Create(columns[c.Item1], c.Item2)).ToList();
            _primaryKeys = new HashSet<string>(PartitionKeys.Select(p => p.ColumnName).Concat(ClusteringKeys.Select(c => c.Item1.ColumnName)));

            MissingPrimaryKeyColumns = new List<string>();
            if (PartitionKeys.Count != partitionkeys.Length)
            {
                MissingPrimaryKeyColumns.AddRange(partitionkeys.Where(k => !columns.Contains(k)));
            }
            if (ClusteringKeys.Count != clusteringKeys.Length)
            {
                MissingPrimaryKeyColumns.AddRange(partitionkeys.Where(k => !columns.Contains(k)));
            }
        }

        /// <summary>
        /// Gets only the PocoColumns from the collection of all columns that are NOT part of the partition or clustering keys.
        /// </summary>
        /// <returns></returns>
        public IList<PocoColumn> GetNonPrimaryKeyColumns()
        {
            // Since the underlying collection (Columns) maintains order, this should be consistent in ordering
            return Columns.Where(c => _primaryKeys.Contains(c.ColumnName) == false).ToList();
        }

        /// <summary>
        /// Gets only the PocoColumns from the collection of all columns that are uniquely identifies a cql row.
        /// First partition and then clustering keys.
        /// </summary>
        public IList<PocoColumn> GetPrimaryKeyColumns()
        {
            // Since the underlying collection (Columns) maintains order, this should be consistent in ordering
            return Columns.Where(c => _primaryKeys.Contains(c.ColumnName)).ToList();
        }

        /// <summary>
        /// Gets the column information for a given member name
        /// </summary>
        public PocoColumn GetColumnByMemberName(string memberName)
        {
            _columnsByMemberName.TryGetValue(memberName, out PocoColumn column);
            return column;
        }

        /// <summary>
        /// Gets the column name for a given member name
        /// </summary>
        public string GetColumnNameByMemberName(string memberName)
        {
            var column = GetColumnByMemberName(memberName);
            return column?.ColumnName;
        }

        /// <summary>
        /// Gets the column name for a given member
        /// </summary>
        public string GetColumnName(MemberInfo member)
        {
            var column = GetColumnByMemberName(member.Name);
            return column?.ColumnName;
        }
    }
}