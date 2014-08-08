using System;
using System.Linq;
using System.Reflection;

namespace CqlPoco.Mapping
{
    internal class ColumnDefinition
    {
        public MemberInfo MemberInfo { get; private set; }
        public Type MemberInfoType { get; private set; }

        public string ColumnName { get; set; }
        public Type ColumnType { get; set; }
        public bool Ignore { get; set; }
        public bool IsExplicitlyDefined { get; set; }

        public ColumnDefinition(FieldInfo fieldInfo)
        {
            MemberInfo = fieldInfo;
            MemberInfoType = fieldInfo.FieldType;
        }

        public ColumnDefinition(PropertyInfo propertyInfo)
        {
            MemberInfo = propertyInfo;
            MemberInfoType = propertyInfo.PropertyType;
        }

        /// <summary>
        /// Creates a ColumnDefinition for a field and initializes the settings based on any supported custom attributes applied to the field.
        /// </summary>
        public static ColumnDefinition FromFieldAttributes(FieldInfo fieldInfo)
        {
            var columnDefinition = new ColumnDefinition(fieldInfo);
            InitFromAttributes(columnDefinition);
            return columnDefinition;
        }

        /// <summary>
        /// Creates a ColumnDefinition for a property and initializes the settings based on any supported custom attributes applied to the property.
        /// </summary>
        public static ColumnDefinition FromPropertyAttributes(PropertyInfo propInfo)
        {
            var columnDefinition = new ColumnDefinition(propInfo);
            InitFromAttributes(columnDefinition);
            return columnDefinition;
        }

        private static void InitFromAttributes(ColumnDefinition columnDefinition)
        {
            ColumnAttribute columnAttribute = columnDefinition.MemberInfo.GetCustomAttributes<ColumnAttribute>(true).FirstOrDefault();
            if (columnAttribute != null)
            {
                columnDefinition.IsExplicitlyDefined = true;

                if (columnAttribute.Name != null)
                    columnDefinition.ColumnName = columnAttribute.Name;

                if (columnAttribute.Type != null)
                    columnDefinition.ColumnType = columnAttribute.Type;
            }

            IgnoreAttribute ignoreAttribute = columnDefinition.MemberInfo.GetCustomAttributes<IgnoreAttribute>(true).FirstOrDefault();
            if (ignoreAttribute != null)
                columnDefinition.Ignore = true;
        }
    }
}