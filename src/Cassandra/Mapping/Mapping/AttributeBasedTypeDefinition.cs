using System;
using System.Linq;
using System.Reflection;

namespace Cassandra.Mapping.Mapping
{
    /// <summary>
    /// A type definition that uses attributes on the class to determine its settings.
    /// </summary>
    public class AttributeBasedTypeDefinition : ITypeDefinition
    {
        private readonly Type _pocoType;
        private readonly string _tableName;
        private readonly bool _explicitColumns;
        private readonly string[] _primaryKeyColumns;

        Type ITypeDefinition.PocoType
        {
            get { return _pocoType; }
        }

        string ITypeDefinition.TableName
        {
            get { return _tableName; }
        }

        bool ITypeDefinition.ExplicitColumns
        {
            get { return _explicitColumns; }
        }

        string[] ITypeDefinition.PrimaryKeyColumns
        {
            get { return _primaryKeyColumns; }
        }

        public bool CaseSensitive { get; set; }

        /// <summary>
        /// Creates a new TypeDefinition for the POCO Type specified using any attributes on the class to determine mappings.
        /// </summary>
        public AttributeBasedTypeDefinition(Type pocoType) 
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            _pocoType = pocoType;

            // Look for supported attributes on the Type and set any properties appropriately
            var primaryKeyAttribute = (PrimaryKeyAttribute)pocoType.GetCustomAttributes(typeof(PrimaryKeyAttribute), true).FirstOrDefault();
            if (primaryKeyAttribute != null)
                _primaryKeyColumns = primaryKeyAttribute.ColumnNames;

            var explicitColumnsAttribute = (ExplicitColumnsAttribute)pocoType.GetCustomAttributes(typeof(ExplicitColumnsAttribute), true).FirstOrDefault();
            if (explicitColumnsAttribute != null)
                _explicitColumns = true;

            var tableNameAttribute = (TableNameAttribute)pocoType.GetCustomAttributes(typeof(TableNameAttribute), true).FirstOrDefault();
            if (tableNameAttribute != null)
                _tableName = tableNameAttribute.Value;
        }

        IColumnDefinition ITypeDefinition.GetColumnDefinition(FieldInfo field)
        {
            return new AttributeBasedColumnDefinition(field);
        }

        IColumnDefinition ITypeDefinition.GetColumnDefinition(PropertyInfo property)
        {
            return new AttributeBasedColumnDefinition(property);
        }
    }
}