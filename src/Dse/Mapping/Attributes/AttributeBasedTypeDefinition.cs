//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dse.Mapping.Attributes
{
    /// <summary>
    /// A type definition that uses attributes on the class to determine its settings.
    /// </summary>
    internal class AttributeBasedTypeDefinition : ITypeDefinition
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;
        public Type PocoType { get; private set; }
        public string TableName { get; private set; }
        public string KeyspaceName { get; private set; }
        public bool ExplicitColumns { get; internal set; }
        public string[] PartitionKeys { get; private set; }
        public Tuple<string, SortOrder>[] ClusteringKeys { get; private set; }
        public bool CaseSensitive { get; private set; }
        public bool CompactStorage { get; private set; }
        public bool AllowFiltering { get; private set; }

        /// <summary>
        /// Creates a new TypeDefinition for the POCO Type specified using any attributes on the class to determine mappings.
        /// </summary>
        public AttributeBasedTypeDefinition(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }
            PocoType = type;
            //Get the table name from the attribute or the type name
            TableName = type.Name;
            var tableAttribute = (TableAttribute)type.GetTypeInfo().GetCustomAttribute(typeof(TableAttribute), true);
            if (tableAttribute != null)
            {
                TableName = tableAttribute.Name;
                KeyspaceName = tableAttribute.Keyspace;
                CaseSensitive = tableAttribute.CaseSensitive;
                CompactStorage = tableAttribute.CompactStorage;
                AllowFiltering = tableAttribute.AllowFiltering;
                ExplicitColumns = tableAttribute.ExplicitColumns;
            }

            //Fields and properties that can be mapped
            var mappable = type
                .GetTypeInfo()
                .GetFields(PublicInstanceBindingFlags)
                .Where(field => field.IsInitOnly == false)
                .Select(field => (MemberInfo)field)
                .Concat(type.GetTypeInfo().GetProperties(PublicInstanceBindingFlags).Where(p => p.CanWrite));
            var partitionKeys = new List<Tuple<string, int>>();
            var clusteringKeys = new List<Tuple<string, SortOrder, int>>();
            foreach (var member in mappable)
            {
                var columnName = member.Name;
                var columnAttribute = (ColumnAttribute)member.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();
                if (columnAttribute?.Name != null)
                {
                    columnName = columnAttribute.Name;
                }
                var partitionKeyAttribute = (PartitionKeyAttribute)member.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault();
                if (partitionKeyAttribute != null)
                {
                    partitionKeys.Add(Tuple.Create(columnName, partitionKeyAttribute.Index));
                    continue;
                }
                var clusteringKeyAttribute = (ClusteringKeyAttribute)member.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault();
                if (clusteringKeyAttribute != null)
                {
                    if (clusteringKeyAttribute.Name != null)
                    {
                        columnName = clusteringKeyAttribute.Name;
                        if (columnAttribute != null && columnAttribute.Name != null &&
                            columnAttribute.Name != clusteringKeyAttribute.Name)
                        {
                            // It uses both [Column] and [ClusteringKey] attributes with different column names
                            throw new InvalidOperationException(string.Format(
                                "The member {0} has [Column] and [ClusteringKey] attributes defined with different" +
                                "column names: '{1}' vs '{2}",
                                member.Name,
                                columnAttribute.Name,
                                clusteringKeyAttribute.Name));
                        }
                    }
                    clusteringKeys.Add(Tuple.Create(columnName, clusteringKeyAttribute.ClusteringSortOrder, clusteringKeyAttribute.Index));
                }
            }

            PartitionKeys = partitionKeys
                //Order the partition keys by index
                .OrderBy(k => k.Item2)
                .Select(k => k.Item1).ToArray();

            ClusteringKeys = clusteringKeys.
                OrderBy(k => k.Item3)
                .Select(k => Tuple.Create(k.Item1, k.Item2))
                .ToArray();
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