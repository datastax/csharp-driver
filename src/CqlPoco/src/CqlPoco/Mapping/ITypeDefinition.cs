using System;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A definition for how to map a POCO.
    /// </summary>
    public interface ITypeDefinition
    {
        /// <summary>
        /// The Type of the POCO.
        /// </summary>
        Type PocoType { get; }

        /// <summary>
        /// The name of the table to map the POCO to.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// Whether or not this POCO should only have columns explicitly defined mapped.
        /// </summary>
        bool ExplicitColumns { get; }

        /// <summary>
        /// The primary key columns.
        /// </summary>
        string[] PrimaryKeyColumns { get; }

        /// <summary>
        /// Gets a column definition for the given field on the POCO.
        /// </summary>
        IColumnDefinition GetColumnDefinition(FieldInfo field);

        /// <summary>
        /// Gets a column definition for the given property on the POCO.
        /// </summary>
        IColumnDefinition GetColumnDefinition(PropertyInfo property);
    }
}
