using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Cassandra.Mapping.Mapping;

namespace Cassandra.Data.Linq
{
    internal class LinqAttributeBasedColumnDefinition : IColumnDefinition
    {
        public MemberInfo MemberInfo { get; private set; }
        public Type MemberInfoType { get; private set; }
        public string ColumnName { get; private set; }
        public Type ColumnType { get; private set; }
        public bool Ignore { get; private set; }
        public bool IsExplicitlyDefined { get; private set; }
        /// <summary>
        /// Creates a new column definition for the field specified using any attributes on the field to determine mapping configuration.
        /// </summary>
        public LinqAttributeBasedColumnDefinition(FieldInfo fieldInfo) 
            : this((MemberInfo) fieldInfo)
        {
            MemberInfoType = fieldInfo.FieldType;
        }

        /// <summary>
        /// Creates a new column definition for the property specified using any attributes on the property to determine mapping configuration.
        /// </summary>
        public LinqAttributeBasedColumnDefinition(PropertyInfo propertyInfo) 
            : this((MemberInfo) propertyInfo)
        {
            MemberInfoType = propertyInfo.PropertyType;
        }

        private LinqAttributeBasedColumnDefinition(MemberInfo memberInfo)
        {
            MemberInfo = memberInfo;

            var columnAttribute = (ColumnAttribute) memberInfo.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();
            if (columnAttribute != null)
            {
                IsExplicitlyDefined = true;

                if (columnAttribute.Name != null)
                {
                    ColumnName = columnAttribute.Name;
                }
            }

            var ignoreAttribute = memberInfo.GetCustomAttributes(typeof(Cassandra.Mapping.IgnoreAttribute), true).FirstOrDefault();
            if (ignoreAttribute != null)
            {
                Ignore = true;
            }
        }
    }
}
