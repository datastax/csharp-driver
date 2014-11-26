using System;
using System.Collections.Generic;
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
        private readonly string[] _partitionKeys;
        private readonly Tuple<string, SortOrder>[] _clusteringKeys = new Tuple<string, SortOrder>[0];

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

        string[] ITypeDefinition.PartitionKeys
        {
            get { return _partitionKeys; }
        }

        Tuple<string, SortOrder>[] ITypeDefinition.ClusteringKeys
        {
            get { return _clusteringKeys; }
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
            {
                //TODO: Use partition and clustering key attributes
                //Until Linq and Mapper attributes are consolidated it doesn't make much sense.
                _partitionKeys = primaryKeyAttribute.ColumnNames;
            }

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