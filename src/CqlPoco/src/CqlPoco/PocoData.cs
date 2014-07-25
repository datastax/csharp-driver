using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using CqlPoco.Statements;

namespace CqlPoco
{
    internal class PocoData
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;
        private static readonly ConcurrentDictionary<Type, PocoData> Cache = new ConcurrentDictionary<Type, PocoData>();

        public MapperFactory MapperFactory { get; private set; }
        public StatementFactory StatementFactory { get; private set; }

        public Type PocoType { get; private set; }
        public Dictionary<string, PocoColumn> Columns { get; private set; }
        
        private PocoData(Type pocoType, Dictionary<string, PocoColumn> columns)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            if (columns == null) throw new ArgumentNullException("columns");
            PocoType = pocoType;
            Columns = columns;

            MapperFactory = new MapperFactory(this);
            StatementFactory = new StatementFactory(this);
        }

        public static PocoData ForType<T>()
        {
            return Cache.GetOrAdd(typeof (T), t => CreatePocoData(t));
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
            return t.GetProperties(PublicInstanceBindingFlags);
        }
    }
}