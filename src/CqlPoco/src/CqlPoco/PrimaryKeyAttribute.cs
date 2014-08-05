using System;

namespace CqlPoco
{
    /// <summary>
    /// An attribute used to indicate the primary key column names for the table a POCO is mapped to.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class PrimaryKeyAttribute : Attribute
    {
        private readonly string[] _columnNames;

        /// <summary>
        /// The column names of the primary key columns for the table.
        /// </summary>
        public string[] ColumnNames
        {
            get { return _columnNames; }
        }

        /// <summary>
        /// Specify the primary key column names (in order) for the table.
        /// </summary>
        /// <param name="columnNames">The column names for the table's primary key.</param>
        public PrimaryKeyAttribute(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length < 1) throw new ArgumentOutOfRangeException("columnNames", "You must specify at least one primary key column name.");

            _columnNames = columnNames;
        }
    }
}
