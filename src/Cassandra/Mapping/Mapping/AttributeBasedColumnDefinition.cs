using System;
using System.Linq;
using System.Reflection;

namespace Cassandra.Mapping.Mapping
{
    /// <summary>
    /// A column definition that uses attributes on the field/property to get its settings.
    /// </summary>
    public class AttributeBasedColumnDefinition : IColumnDefinition
    {
        private readonly MemberInfo _memberInfo;
        private readonly Type _memberInfoType;

        private readonly string _columnName;
        private readonly Type _columnType;
        private readonly bool _ignore;
        private readonly bool _isExplicitlyDefined;
        private bool _secondaryIndex = false;
        private bool _isCounter = false;

        MemberInfo IColumnDefinition.MemberInfo
        {
            get { return _memberInfo; }
        }

        Type IColumnDefinition.MemberInfoType
        {
            get { return _memberInfoType; }
        }

        string IColumnDefinition.ColumnName
        {
            get { return _columnName; }
        }

        Type IColumnDefinition.ColumnType
        {
            get { return _columnType; }
        }

        bool IColumnDefinition.Ignore
        {
            get { return _ignore; }
        }

        bool IColumnDefinition.IsExplicitlyDefined
        {
            get { return _isExplicitlyDefined; }
        }

        bool IColumnDefinition.SecondaryIndex
        {
            get { return _secondaryIndex; }
        }

        bool IColumnDefinition.IsCounter
        {
            get { return _isCounter; }
        }

        /// <summary>
        /// Creates a new column definition for the field specified using any attributes on the field to determine mapping configuration.
        /// </summary>
        public AttributeBasedColumnDefinition(FieldInfo fieldInfo) 
            : this((MemberInfo) fieldInfo)
        {
            _memberInfoType = fieldInfo.FieldType;
        }

        /// <summary>
        /// Creates a new column definition for the property specified using any attributes on the property to determine mapping configuration.
        /// </summary>
        public AttributeBasedColumnDefinition(PropertyInfo propertyInfo) 
            : this((MemberInfo) propertyInfo)
        {
            _memberInfoType = propertyInfo.PropertyType;
        }

        private AttributeBasedColumnDefinition(MemberInfo memberInfo)
        {
            _memberInfo = memberInfo;

            var columnAttribute = (ColumnAttribute) memberInfo.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();
            if (columnAttribute != null)
            {
                _isExplicitlyDefined = true;

                if (columnAttribute.Name != null)
                    _columnName = columnAttribute.Name;

                if (columnAttribute.Type != null)
                    _columnType = columnAttribute.Type;
            }
            //TODO: Handle Counter, Partition and secondary indexes
            var ignoreAttribute = memberInfo.GetCustomAttributes(typeof(IgnoreAttribute), true).FirstOrDefault();
            if (ignoreAttribute != null)
                _ignore = true;
        }
    }
}