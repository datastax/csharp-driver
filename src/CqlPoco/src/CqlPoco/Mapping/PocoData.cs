using System;
using CqlPoco.Utils;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// Represents data about a POCO and its mapping to Cassandra Rows in a table.
    /// </summary>
    internal class PocoData
    {
        public Type PocoType { get; private set; }
        public string TableName { get; private set; }
        public LookupKeyedCollection<string, PocoColumn> Columns { get; private set; }

        public PocoData(Type pocoType, string tableName, LookupKeyedCollection<string, PocoColumn> columns)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (columns == null) throw new ArgumentNullException("columns");
            PocoType = pocoType;
            TableName = tableName;
            Columns = columns;
        }
    }
}