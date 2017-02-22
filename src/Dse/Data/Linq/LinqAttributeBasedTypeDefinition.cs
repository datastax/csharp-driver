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
using System.Text;
using Dse.Mapping;

namespace Dse.Data.Linq
{
    /// <summary>
    /// A type definition that uses Linq attributes on the class to determine its settings.
    /// It uses Linq default backward-compatible settings (like case sensitivity)
    /// </summary>
    [Obsolete]
    internal class LinqAttributeBasedTypeDefinition : ITypeDefinition
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;
        public Type PocoType { get; private set; }
        public string TableName { get; private set; }
        public string KeyspaceName { get; private set; }
        public bool ExplicitColumns { get; private set; }
        public string[] PartitionKeys { get; private set; }
        public Tuple<string, SortOrder>[] ClusteringKeys { get; private set; }
        public bool CaseSensitive { get; private set; }
        public bool CompactStorage { get; private set; }
        public bool AllowFiltering { get; private set; }

        public LinqAttributeBasedTypeDefinition(Type type, string tableName, string keyspaceName)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }
            PocoType = type;
            CaseSensitive = true;
            ExplicitColumns = false;
            TableName = tableName;
            KeyspaceName = keyspaceName;

            //Fields and properties that can be mapped
            var mappable = type
                .GetTypeInfo()
                .GetFields(PublicInstanceBindingFlags)
                .Where(field => field.IsInitOnly == false)
                .Select(field => (MemberInfo) field)
                .Concat(type.GetTypeInfo().GetProperties(PublicInstanceBindingFlags).Where(p => p.CanWrite));
            var partitionKeys = new List<Tuple<string, int>>();
            var clusteringKeys = new List<Tuple<string, SortOrder, int>>();
            foreach (var member in mappable)
            {
                var columnName = member.Name;
                var columnAttribute = (ColumnAttribute) member.GetCustomAttributes(typeof (ColumnAttribute), true).FirstOrDefault();
                if (columnAttribute != null)
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

            //Get the table name from the attribute or the type name
            if (TableName == null)
            {
                TableName = type.Name;
                var tableAttribute = (TableAttribute)type.GetTypeInfo().GetCustomAttribute(typeof(TableAttribute), true);
                if (tableAttribute != null)
                {
                    TableName = tableAttribute.Name;
                    CaseSensitive = tableAttribute.CaseSensitive;
                }
            }
            if (type.GetTypeInfo().GetCustomAttribute(typeof(CompactStorageAttribute), true) != null)
            {
                CompactStorage = true;
            }
            if (type.GetTypeInfo().GetCustomAttribute(typeof(AllowFilteringAttribute), true) != null)
            {
                AllowFiltering = true;
            }
        }

        internal static ITypeDefinition DetermineAttributes(Type type)
        {
            if (type.GetTypeInfo().GetCustomAttributes(typeof(TableAttribute), true).Any())
            {
                return new LinqAttributeBasedTypeDefinition(type, null, null);
            }
            //Use the default mapping attributes
            return new Mapping.Attributes.AttributeBasedTypeDefinition(type);
        }

        public IColumnDefinition GetColumnDefinition(FieldInfo field)
        {
            return new LinqAttributeBasedColumnDefinition(field);
        }

        public IColumnDefinition GetColumnDefinition(PropertyInfo property)
        {
            return new LinqAttributeBasedColumnDefinition(property);
        }
    }
}
