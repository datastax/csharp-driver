using System;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A definition for a POCO column (i.e. property or field).
    /// </summary>
    public abstract class ColumnDefinition
    {
        /// <summary>
        /// The MemberInfo for the property or field.
        /// </summary>
        protected internal MemberInfo MemberInfo { get; set; }

        /// <summary>
        /// The Type of the property or field (i.e. FieldInfo.FieldType or PropertyInfo.PropertyType).
        /// </summary>
        protected internal Type MemberInfoType { get; set; }

        /// <summary>
        /// The name of the column in the database that this property/field maps to.
        /// </summary>
        protected internal abstract string ColumnName { get; }

        /// <summary>
        /// The data type of the column in C* for inserting/updating data.
        /// </summary>
        protected internal abstract Type ColumnType { get; }

        /// <summary>
        /// Whether the property/field should be ignored when mapping.
        /// </summary>
        protected internal abstract bool Ignore { get; }

        /// <summary>
        /// Whether or not this column has been explicitly defined (for use when TypeDefinition.ExplicitColumns is true).
        /// </summary>
        protected internal abstract bool IsExplicitlyDefined { get; }

        protected ColumnDefinition(FieldInfo fieldInfo)
        {
            MemberInfo = fieldInfo;
            MemberInfoType = fieldInfo.FieldType;
        }

        protected ColumnDefinition(PropertyInfo propertyInfo)
        {
            MemberInfo = propertyInfo;
            MemberInfoType = propertyInfo.PropertyType;
        }
    }
}