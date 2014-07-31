using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlPoco
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
            // Find all public instance fields and properties and convert to PocoColumn dictionary keyed by column name
            Dictionary<string, PocoColumn> columns = GetPocoFields(pocoType).Select(PocoColumn.FromField)
                                                                            .Union(GetPocoProperties(pocoType).Select(PocoColumn.FromProperty))
                                                                            .ToDictionary(pc => pc.ColumnName, StringComparer.OrdinalIgnoreCase);
            return new PocoData(pocoType, columns);
        }

        /// <summary>
        /// Gets any public instance fields that are settable for the given type.
        /// </summary>
        private static IEnumerable<FieldInfo> GetPocoFields(Type t)
        {
            return t.GetFields(PublicInstanceBindingFlags).Where(field => field.IsInitOnly == false);
        }

        /// <summary>
        /// Gets any public instance properties for the given type.
        /// </summary>
        private static IEnumerable<PropertyInfo> GetPocoProperties(Type t)
        {
            return t.GetProperties(PublicInstanceBindingFlags).Where(p => p.CanWrite);
        }
    }
}