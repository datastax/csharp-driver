using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using CqlPoco.Utils;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// Factory responsible for creating PocoData instances.
    /// </summary>
    internal class PocoDataFactory
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;

        private readonly ConcurrentDictionary<Type, PocoData> _cache; 

        public PocoDataFactory()
        {
            _cache = new ConcurrentDictionary<Type, PocoData>();
        }

        public PocoData GetPocoData<T>()
        {
            return _cache.GetOrAdd(typeof(T), CreatePocoData);
        }
        
        private static PocoData CreatePocoData(Type pocoType)
        {
            // TODO:  Allow fluent mappings to be defined
            TypeDefinition typeDefinition = null;

            // No fluent mapping defined, so get from attributes
            if (typeDefinition == null)
                typeDefinition = new AttributeBasedTypeDefinition(pocoType);

            // Figure out the table name (if not specified, use the POCO class' name)
            string tableName = typeDefinition.TableName ?? pocoType.Name;

            // Figure out the primary key columns (if not specified, assume a column called "id" is used)
            string[] pkColumnNames = typeDefinition.PrimaryKeyColumns ?? new[] { "id" };
            var primaryKeyColumns = new HashSet<string>(pkColumnNames, StringComparer.OrdinalIgnoreCase);

            // Create PocoColumn collection (where ordering is guaranteed to be consistent) with PK columns LAST
            LookupKeyedCollection<string, PocoColumn> columns = typeDefinition.GetColumnDefinitions()
                                                                              .Select(PocoColumn.FromColumnDefinition)
                                                                              .OrderBy(pc => primaryKeyColumns.Contains(pc.ColumnName))
                                                                              .ToLookupKeyedCollection(pc => pc.ColumnName,
                                                                                                       StringComparer.OrdinalIgnoreCase);

            return new PocoData(pocoType, tableName, columns, primaryKeyColumns);
        }

        /// <summary>
        /// Gets any public instance fields that are settable for the given type.
        /// </summary>
        private static IEnumerable<FieldInfo> GetMappableFields(Type t)
        {
            return t.GetFields(PublicInstanceBindingFlags).Where(field => field.IsInitOnly == false);
        }

        /// <summary>
        /// Gets any public instance properties for the given type.
        /// </summary>
        private static IEnumerable<PropertyInfo> GetMappableProperties(Type t)
        {
            return t.GetProperties(PublicInstanceBindingFlags).Where(p => p.CanWrite);
        }
    }
}