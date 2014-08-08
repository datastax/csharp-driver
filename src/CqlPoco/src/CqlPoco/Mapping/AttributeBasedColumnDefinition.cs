using System;
using System.Linq;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A column definition that uses attributes on the field/property to get its settings.
    /// </summary>
    public class AttributeBasedColumnDefinition : ColumnDefinition
    {
        private string _columnName;
        private Type _columnType;
        private bool _ignore;
        private bool _isExplicitlyDefined;

        protected internal override string ColumnName
        {
            get { return _columnName; }
        }

        protected internal override Type ColumnType
        {
            get { return _columnType; }
        }

        protected internal override bool Ignore
        {
            get { return _ignore; }
        }

        protected internal override bool IsExplicitlyDefined
        {
            get { return _isExplicitlyDefined; }
        }

        public AttributeBasedColumnDefinition(FieldInfo fieldInfo) 
            : base(fieldInfo)
        {
            InitFromAttributes(fieldInfo);
        }

        public AttributeBasedColumnDefinition(PropertyInfo propertyInfo) 
            : base(propertyInfo)
        {
            InitFromAttributes(propertyInfo);
        }

        private void InitFromAttributes(MemberInfo memberInfo)
        {
            ColumnAttribute columnAttribute = memberInfo.GetCustomAttributes<ColumnAttribute>(true).FirstOrDefault();
            if (columnAttribute != null)
            {
                _isExplicitlyDefined = true;

                if (columnAttribute.Name != null)
                    _columnName = columnAttribute.Name;

                if (columnAttribute.Type != null)
                    _columnType = columnAttribute.Type;
            }

            IgnoreAttribute ignoreAttribute = memberInfo.GetCustomAttributes<IgnoreAttribute>(true).FirstOrDefault();
            if (ignoreAttribute != null)
                _ignore = true;
        }
    }
}