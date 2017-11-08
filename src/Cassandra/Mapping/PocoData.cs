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
    internal class PocoData
    {
        private readonly Dictionary<string, PocoColumn> _columnsByMemberName;
        private readonly HashSet<string> _primaryKeys;
        private string _keyspaceName;
        private Func<string> _onKeyspaceRequest;
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
        public string KeyspaceName
        {
            get
            {
                if (string.IsNullOrWhiteSpace(_keyspaceName))
                {
                    return _onKeyspaceRequest?.Invoke();
                }
                return _keyspaceName;
                //return _keyspaceName;
            }
            set { _keyspaceName = value; }
        }

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
                        string[] partitionkeys, Tuple<string, SortOrder>[] clusteringKeys, bool caseSensitive, bool compact, bool allowFiltering,Func<string> keyspaceCallback=null)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (columns == null) throw new ArgumentNullException("columns");
            if (partitionkeys == null) throw new ArgumentNullException("partitionkeys");
            if (clusteringKeys == null) throw new ArgumentNullException("clusteringKeys");
            PocoType = pocoType;
            TableName = tableName;
            Columns = columns;
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
            _onKeyspaceRequest = keyspaceCallback;
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
            PocoColumn column;
            _columnsByMemberName.TryGetValue(memberName, out column);
            return column;
        }

        /// <summary>
        /// Gets the column name for a given member name
        /// </summary>
        public string GetColumnNameByMemberName(string memberName)
        {
            var column = GetColumnByMemberName(memberName);
            return column != null ? column.ColumnName : null;
        }

        /// <summary>
        /// Gets the column name for a given member
        /// </summary>
        public string GetColumnName(MemberInfo member)
        {
            var column = GetColumnByMemberName(member.Name);
            return column != null ? column.ColumnName : null;
        }
    }
}