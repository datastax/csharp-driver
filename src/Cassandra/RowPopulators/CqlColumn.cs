using System;

namespace Cassandra
{
    public class CqlColumn : ColumnDesc
    {
        /// <summary>
        /// Index of the column in the rowset
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// CLR Type of the column
        /// </summary>
        public Type Type { get; set; }
    }
}