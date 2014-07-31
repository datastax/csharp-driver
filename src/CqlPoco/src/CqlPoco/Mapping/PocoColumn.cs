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
        
        /// <summary>
        /// Creates a PocoColumn for a field.
        /// </summary>
        internal static PocoColumn FromField(FieldInfo fieldInfo)
        {
            return FromMemberInfo(fieldInfo, fieldInfo.FieldType);
        }

        /// <summary>
        /// Creates a PocoColumn for a property.
        /// </summary>
        internal static PocoColumn FromProperty(PropertyInfo propInfo)
        {
            return FromMemberInfo(propInfo, propInfo.PropertyType);
        }

        private static PocoColumn FromMemberInfo(MemberInfo memberInfo, Type memberInfoType)
        {
            // See if the column name was overridden
            var columnAttribute = memberInfo.GetCustomAttribute<ColumnAttribute>();
            string columnName = columnAttribute == null || string.IsNullOrEmpty(columnAttribute.Name) ? memberInfo.Name : columnAttribute.Name;
            
            return new PocoColumn
            {
                ColumnName = columnName,
                MemberInfo = memberInfo,
                MemberInfoType = memberInfoType
            };
        }
    }
}