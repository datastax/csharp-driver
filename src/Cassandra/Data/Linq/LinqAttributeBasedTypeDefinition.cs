//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra.Mapping;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// A type definition that uses Linq attributes on the class to determine its settings.
    /// It uses Linq default backward-compatible settings (like case sensitivity)
    /// </summary>
    [Obsolete]
    internal class LinqAttributeBasedTypeDefinition : ITypeDefinition
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;
        public Type PocoType { get; }
        public string TableName { get; }
        public string KeyspaceName { get; }
        public bool ExplicitColumns { get; }
        public string[] PartitionKeys { get; }
        public Tuple<string, SortOrder>[] ClusteringKeys { get; }
        public bool CaseSensitive { get; }
        public bool CompactStorage { get; }
        public bool AllowFiltering { get; }

        public LinqAttributeBasedTypeDefinition(Type type, string tableName, string keyspaceName)
        {
            PocoType = type ?? throw new ArgumentNullException(nameof(type));
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
