using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlPoco.Mapping
{
    internal class TypeDefinition
    {
        private const BindingFlags PublicInstanceBindingFlags = BindingFlags.Public | BindingFlags.Instance;

        public Type PocoType { get; private set; }
        public Dictionary<MemberInfo, ColumnDefinition> ColumnDefinitions { get; private set; }

        public string TableName { get; set; }
        public bool ExplicitColumns { get; set; }
        public string[] PrimaryKeyColumns { get; set; }

        public TypeDefinition(Type pocoType)
            : this(pocoType, fi => new ColumnDefinition(fi), pi => new ColumnDefinition(pi))
        {
        }

        private TypeDefinition(Type pocoType, Func<FieldInfo, ColumnDefinition> getColumnDefinitionFromField,
                               Func<PropertyInfo, ColumnDefinition> getColumnDefinitionFromProperty)
        {
            if (pocoType == null) throw new ArgumentNullException("pocoType");
            PocoType = pocoType;

            IEnumerable<ColumnDefinition> fields = GetMappableFields(pocoType).Select(getColumnDefinitionFromField);
            IEnumerable<ColumnDefinition> props = GetMappableProperties(pocoType).Select(getColumnDefinitionFromProperty);
            ColumnDefinitions = fields.Union(props).ToDictionary(cd => cd.MemberInfo);
        }

        /// <summary>
        /// Creates a TypeDefinition for the Type specified with the settings derived from any custom attributes applied to the class.
        /// </summary>
        public static TypeDefinition FromAttributes(Type pocoType)
        {
            var typeDefinition = new TypeDefinition(pocoType, ColumnDefinition.FromFieldAttributes, ColumnDefinition.FromPropertyAttributes);

            // Look for supported attributes on the Type and set any properties appropriately
            PrimaryKeyAttribute primaryKeyAttribute = pocoType.GetCustomAttributes<PrimaryKeyAttribute>(true).FirstOrDefault();
            if (primaryKeyAttribute != null)
                typeDefinition.PrimaryKeyColumns = primaryKeyAttribute.ColumnNames;

            ExplicitColumnsAttribute explicitColumnsAttribute = pocoType.GetCustomAttributes<ExplicitColumnsAttribute>(true).FirstOrDefault();
            if (explicitColumnsAttribute != null)
                typeDefinition.ExplicitColumns = true;

            TableNameAttribute tableNameAttribute = pocoType.GetCustomAttributes<TableNameAttribute>(true).FirstOrDefault();
            if (tableNameAttribute != null)
                typeDefinition.TableName = tableNameAttribute.Value;

            return typeDefinition;
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
