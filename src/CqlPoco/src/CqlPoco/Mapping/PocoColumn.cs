using System;
using System.Reflection;

namespace CqlPoco.Mapping
{
    internal class PocoColumn
    {
        /// <summary>
        /// The name of the column in the database.
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// The data type of the column in the database for use when inserting/updating.
        /// </summary>
        public Type ColumnType { get; private set; }

        /// <summary>
        /// The MemberInfo for the POCO field/property.
        /// </summary>
        public MemberInfo MemberInfo { get; private set; }

        /// <summary>
        /// The .NET Type of the POCO field/property (i.e. FieldInfo.FieldType or PropertyInfo.PropertyType)
        /// </summary>
        public Type MemberInfoType { get; private set; }

        private PocoColumn()
        {
        }

        public static PocoColumn FromColumnDefinition(IColumnDefinition columnDefinition)
        {
            return new PocoColumn
            {
                // Default the column name to the prop/field name if not specified
                ColumnName = columnDefinition.ColumnName ?? columnDefinition.MemberInfo.Name,
                // Default the column type to the prop/field type if not specified
                ColumnType = columnDefinition.ColumnType ?? columnDefinition.MemberInfoType,
                MemberInfo = columnDefinition.MemberInfo,
                MemberInfoType = columnDefinition.MemberInfoType
            };
        }
    }
}