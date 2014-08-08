using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// A definition for a POCO.
    /// </summary>
    public abstract class TypeDefinition
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;

        /// <summary>
        /// The Type of the POCO.
        /// </summary>
        protected internal Type PocoType { get; set; }

        /// <summary>
        /// The name of the table to map the POCO to.
        /// </summary>
        protected internal abstract string TableName { get; }

        /// <summary>
        /// Whether or not this POCO should only have columns explicitly defined mapped.
        /// </summary>
        protected internal abstract bool ExplicitColumns { get; }

        /// <summary>
        /// The primary key columns.
        /// </summary>
        protected internal abstract string[] PrimaryKeyColumns { get; }

        protected TypeDefinition(Type pocoType)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            PocoType = pocoType;
        }

        /// <summary>
        /// Gets all the column definitions for columns that should be mapped for this POCO.
        /// </summary>
        internal IEnumerable<ColumnDefinition> GetColumnDefinitions()
        {
            // Get column definitions for all mappable fields and properties
            IEnumerable<ColumnDefinition> fieldsAndProperties = GetMappableFields(PocoType)
                .Select(GetColumnDefinition)
                .Union(GetMappableProperties(PocoType).Select(GetColumnDefinition));

            // If explicit columns, only get column definitions that are explicitly defined, otherwise get all columns that aren't marked as Ignored
            return ExplicitColumns
                       ? fieldsAndProperties.Where(c => c.IsExplicitlyDefined)
                       : fieldsAndProperties.Where(c => c.Ignore == false);
        }

        /// <summary>
        /// Gets a column definition for the given field on the POCO.
        /// </summary>
        protected abstract ColumnDefinition GetColumnDefinition(FieldInfo field);

        /// <summary>
        /// Gets a column definition for the given property on the POCO.
        /// </summary>
        protected abstract ColumnDefinition GetColumnDefinition(PropertyInfo property);

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
