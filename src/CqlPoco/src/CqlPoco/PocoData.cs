using System;
using System.Collections.Generic;

namespace CqlPoco
{
    /// <summary>
    /// Represents data about a POCO and its mapping to Cassandra Rows in a table.
    /// </summary>
    internal class PocoData
    {
        public Type PocoType { get; private set; }
        public Dictionary<string, PocoColumn> Columns { get; private set; }

        public PocoData(Type pocoType, Dictionary<string, PocoColumn> columns)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            if (columns == null) throw new ArgumentNullException("columns");
            PocoType = pocoType;
            Columns = columns;
        }
    }
}