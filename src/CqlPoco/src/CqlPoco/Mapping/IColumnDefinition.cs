using System;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A definition for how a property/field maps to a POCO.
    /// </summary>
    public interface IColumnDefinition
    {
        /// <summary>
        /// The MemberInfo for the property or field.
        /// </summary>
        MemberInfo MemberInfo { get; }

        /// <summary>
        /// The Type of the property or field (i.e. FieldInfo.FieldType or PropertyInfo.PropertyType).
        /// </summary>
        Type MemberInfoType { get; }

        /// <summary>
        /// The name of the column in the database that this property/field maps to.
        /// </summary>
        string ColumnName { get; }

        /// <summary>
        /// The data type of the column in C* for inserting/updating data.
        /// </summary>
        Type ColumnType { get; }

        /// <summary>
        /// Whether the property/field should be ignored when mapping.
        /// </summary>
        bool Ignore { get; }

        /// <summary>
        /// Whether or not this column has been explicitly defined (for use when TypeDefinition.ExplicitColumns is true).
        /// </summary>
        bool IsExplicitlyDefined { get; }
    }
}