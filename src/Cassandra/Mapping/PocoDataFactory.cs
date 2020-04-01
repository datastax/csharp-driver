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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra.Mapping.Attributes;
using Cassandra.Mapping.Utils;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Factory responsible for creating PocoData instances, uses AttributeBasedTypeDefinition to create new Poco information in case a definition was not provided.
    /// </summary>
    internal class PocoDataFactory
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;

        private readonly LookupKeyedCollection<Type, ITypeDefinition> _predefinedTypeDefinitions;
        private readonly ConcurrentDictionary<Type, PocoData> _cache;

        /// <summary>
        /// Creates a new factory responsible of PocoData instances.
        /// </summary>
        /// <param name="predefinedTypeDefinitions">Explicitly declared type definitions</param>
        public PocoDataFactory(LookupKeyedCollection<Type, ITypeDefinition> predefinedTypeDefinitions)
        {
            _predefinedTypeDefinitions = predefinedTypeDefinitions ?? throw new ArgumentNullException("predefinedTypeDefinitions");
            _cache = new ConcurrentDictionary<Type, PocoData>();
        }

        public PocoData GetPocoData<T>()
        {
            return _cache.GetOrAdd(typeof(T), CreatePocoData);
        }

        /// <summary>
        /// Adds a definition to the local state in case no definition was explicitly defined.
        /// Used when the local default (AttributeBasedTypeDefinition) is not valid for a given type.
        /// </summary>
        public void AddDefinitionDefault(Type type, Func<ITypeDefinition> definitionHandler)
        {
            //In case there isn't already Poco information in the local cache.
            if (_predefinedTypeDefinitions.Contains(type))
            {
                return;
            }
            _cache.GetOrAdd(type, t => CreatePocoData(t, definitionHandler()));
        }
        
        private PocoData CreatePocoData(Type pocoType)
        {
            // Try to get mapping from predefined collection, otherwise fallback to using attributes
            if (!_predefinedTypeDefinitions.TryGetItem(pocoType, out ITypeDefinition typeDefinition))
            {
                typeDefinition = new AttributeBasedTypeDefinition(pocoType);
            }
            return CreatePocoData(pocoType, typeDefinition);
        }

        private PocoData CreatePocoData(Type pocoType, ITypeDefinition typeDefinition)
        {
            // Figure out the table name (if not specified, use the POCO class' name)
            string tableName = typeDefinition.TableName ?? pocoType.Name;

            // Figure out the primary key columns (if not specified, assume a column called "id" is used)
            var pkColumnNames = typeDefinition.PartitionKeys ?? new[] { "id" };

            // Get column definitions for all mappable fields and properties
            IEnumerable<IColumnDefinition> fieldsAndProperties = GetMappableFields(typeDefinition.PocoType)
                .Select(typeDefinition.GetColumnDefinition)
                .Union(GetMappableProperties(typeDefinition.PocoType).Select(typeDefinition.GetColumnDefinition))
                .OrderBy(col => col?.ColumnName ?? col?.MemberInfo.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase);

            // If explicit columns, only get column definitions that are explicitly defined, otherwise get all columns that aren't marked as Ignored
            IEnumerable<IColumnDefinition> columnDefinitions = typeDefinition.ExplicitColumns
                                                                   ? fieldsAndProperties.Where(c => c.IsExplicitlyDefined)
                                                                   : fieldsAndProperties.Where(c => c.Ignore == false);

            // Create PocoColumn collection (where ordering is guaranteed to be consistent)
            LookupKeyedCollection<string, PocoColumn> columns = columnDefinitions
                .Select(PocoColumn.FromColumnDefinition)
                .ToLookupKeyedCollection(pc => pc.ColumnName, StringComparer.OrdinalIgnoreCase);

            var clusteringKeyNames = typeDefinition.ClusteringKeys ?? new Tuple<string, SortOrder>[0];
            return new PocoData(pocoType, tableName, typeDefinition.KeyspaceName, columns, pkColumnNames, clusteringKeyNames, typeDefinition.CaseSensitive, typeDefinition.CompactStorage, typeDefinition.AllowFiltering);
        }

        /// <summary>
        /// Gets any public instance fields that are settable for the given type.
        /// </summary>
        internal static IEnumerable<FieldInfo> GetMappableFields(Type t)
        {
            return t.GetTypeInfo().GetFields(PublicInstanceBindingFlags).Where(field => field.IsInitOnly == false);
        }

        /// <summary>
        /// Gets any public instance properties for the given type.
        /// </summary>
        private static IEnumerable<PropertyInfo> GetMappableProperties(Type t)
        {
            return t.GetTypeInfo().GetProperties(PublicInstanceBindingFlags).Where(p => p.CanWrite);
        }
    }
}