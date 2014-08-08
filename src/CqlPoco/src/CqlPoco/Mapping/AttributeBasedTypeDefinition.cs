using System;
using System.Linq;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A type definition that uses attributes on the class to determine its settings.
    /// </summary>
    public class AttributeBasedTypeDefinition : TypeDefinition
    {
        private readonly string _tableName;
        private readonly bool _explicitColumns;
        private readonly string[] _primaryKeyColumns;

        protected internal override string TableName
        {
            get { return _tableName; }
        }

        protected internal override bool ExplicitColumns
        {
            get { return _explicitColumns; }
        }

        protected internal override string[] PrimaryKeyColumns
        {
            get { return _primaryKeyColumns; }
        }

        public AttributeBasedTypeDefinition(Type pocoType) 
            : base(pocoType)
        {
            // Look for supported attributes on the Type and set any properties appropriately
            PrimaryKeyAttribute primaryKeyAttribute = pocoType.GetCustomAttributes<PrimaryKeyAttribute>(true).FirstOrDefault();
            if (primaryKeyAttribute != null)
                _primaryKeyColumns = primaryKeyAttribute.ColumnNames;

            ExplicitColumnsAttribute explicitColumnsAttribute = pocoType.GetCustomAttributes<ExplicitColumnsAttribute>(true).FirstOrDefault();
            if (explicitColumnsAttribute != null)
                _explicitColumns = true;

            TableNameAttribute tableNameAttribute = pocoType.GetCustomAttributes<TableNameAttribute>(true).FirstOrDefault();
            if (tableNameAttribute != null)
                _tableName = tableNameAttribute.Value;
        }

        protected override ColumnDefinition GetColumnDefinition(FieldInfo field)
        {
            return new AttributeBasedColumnDefinition(field);
        }

        protected override ColumnDefinition GetColumnDefinition(PropertyInfo property)
        {
            return new AttributeBasedColumnDefinition(property);
        }
    }
}