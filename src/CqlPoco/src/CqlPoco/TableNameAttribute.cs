using System;

namespace CqlPoco
{
    /// <summary>
    /// Used to specify the table a POCO maps to.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TableNameAttribute : Attribute
    {
        private readonly string _tableName;

        /// <summary>
        /// The table name.
        /// </summary>
        public string Value
        {
            get { return _tableName; }
        }

        /// <summary>
        /// Specifies the table a POCO maps to.
        /// </summary>
        /// <param name="tableName">The name of the table to map this POCO to.</param>
        public TableNameAttribute(string tableName)
        {
            _tableName = tableName;
        }
    }
}
