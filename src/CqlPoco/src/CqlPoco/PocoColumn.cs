using System;
using System.Reflection;

namespace CqlPoco
{
    public class PocoColumn
    {
        /// <summary>
        /// The name of the column in the database.
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// The .NET type of the data in the database.
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
            return new PocoColumn
            {
                ColumnName = memberInfo.Name,
                ColumnType = memberInfoType,
                MemberInfo = memberInfo,
                MemberInfoType = memberInfoType
            };
        }
    }
}