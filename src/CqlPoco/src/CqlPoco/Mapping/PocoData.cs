using System;
using System.Collections.Generic;
using System.Linq;
using CqlPoco.Utils;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// Represents data about a POCO and its mapping to Cassandra Rows in a table.
    /// </summary>
    internal class PocoData
    {
        /// <summary>
        /// The .NET Type of the POCO this data is for.
        /// </summary>
        public Type PocoType { get; private set; }

        /// <summary>
        /// The table name the POCO maps to in C*.
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// All columns (including PK columns) keyed by their column names and ordered so that the primary key columns are in order last.
        /// </summary>
        public LookupKeyedCollection<string, PocoColumn> Columns { get; private set; }

        /// <summary>
        /// The column names for the primary key columns.
        /// </summary>
        public HashSet<string> PrimaryKeyColumns { get; private set; }

        /// <summary>
        /// The column names of any primary key columns that aren't in the Columns collection.  Could indicate a misconfiguration if the POCO
        /// is going to be used in auto-generated UPDATE/DELETE statements.
        /// </summary>
        public string[] MissingPrimaryKeyColumns { get; private set; }

        public PocoData(Type pocoType, string tableName, LookupKeyedCollection<string, PocoColumn> columns, 
                        HashSet<string> primaryKeyColumns)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (columns == null) throw new ArgumentNullException("columns");
            if (primaryKeyColumns == null) throw new ArgumentNullException("primaryKeyColumns");
            PocoType = pocoType;
            TableName = tableName;
            Columns = columns;
            PrimaryKeyColumns = primaryKeyColumns;

            MissingPrimaryKeyColumns = PrimaryKeyColumns.Where(colName => Columns.Contains(colName) == false).ToArray();
        }

        /// <summary>
        /// Gets only the PocoColumns from the collection of all columns that are NOT PK columns.
        /// </summary>
        /// <returns></returns>
        public IList<PocoColumn> GetNonPrimaryKeyColumns()
        {
            // Since the underlying collection (Columns) maintains order, this should be consistent in ordering
            return Columns.Where(c => PrimaryKeyColumns.Contains(c.ColumnName) == false).ToList();
        }

        /// <summary>
        /// Gets only the PocoColumns from the collection of all columns that are PK columns.
        /// </summary>
        public IList<PocoColumn> GetPrimaryKeyColumns()
        {
            // Since the underlying collection (Columns) maintains order, this should be consistent in ordering
            return Columns.Where(c => PrimaryKeyColumns.Contains(c.ColumnName)).ToList();
        }
    }
}