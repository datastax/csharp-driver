using System;
using System.Reflection;

namespace Cassandra.Mapping
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

        /// <summary>
        /// Determines if there is a secondary index defined for this column
        /// </summary>
        bool SecondaryIndex { get; }

		/// <summary>
		/// Determines if there is a secondary key index defined for this column
		/// </summary>
		bool SecondaryKeyIndex { get; }

        /// <summary>
        /// Determines if this column is a counter column
        /// </summary>
        bool IsCounter { get; }

        /// <summary>
        /// Determines if this column is a static column
        /// </summary>
        bool IsStatic { get; }

        /// <summary>
        /// Determines if the column is frozen.
        /// Only valid for collections, tuples, and user-defined types. For example: frozen&lt;address&gt;
        /// </summary>
        bool IsFrozen { get; }

        /// <summary>
        /// Determines if the key of the column type is frozen.
        /// Only valid for maps and sets, for example: map&lt;frozen&lt;tuple&lt;text, text&gt;&gt;, uuid&gt; .
        /// </summary>
        bool HasFrozenKey { get; }

        /// <summary>
        /// Determines if the value of the column type is frozen.
        /// Only valid for maps and lists, for example: map&lt;uuid, frozen&lt;tuple&lt;text, text&gt;&gt;&gt; .
        /// </summary>
        bool HasFrozenValue { get; }
    }
}