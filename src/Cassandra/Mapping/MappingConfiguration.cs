using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Mapping.Statements;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Mapping.Utils;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Stores the mapping definitions to be used by the Mapper and Linq components.
    /// </summary>
    public sealed class MappingConfiguration
    {
        /// <summary>
        /// Instance to be used for global mappings. It won't get initialized until the first use.
        /// </summary>
        private static readonly MappingConfiguration GlobalInstance = new MappingConfiguration();
        private TypeConverter _typeConverter;
        private LookupKeyedCollection<Type, ITypeDefinition> _typeDefinitions;

        static MappingConfiguration()
        {
            // Explicit static constructor to tell C# compiler
            // not to mark type as beforefieldinit
        }

        /// <summary>
        /// Global mapping definitions to be reused across all the Application Domain.
        /// </summary>
        public static MappingConfiguration Global
        {
            get { return GlobalInstance; }
        }

        /// <summary>
        /// Retrieves the MapperFactory associated with this configuration instance
        /// </summary>
        internal MapperFactory MapperFactory { get; private set; }

        /// <summary>
        /// Retrieves the StatementFactory associated with this configuration instance
        /// </summary>
        internal StatementFactory StatementFactory { get; private set; }

        /// <summary>
        /// Creates a new instance of MappingConfiguration to store the mapping definitions to be used by the Mapper or Linq components.
        /// </summary>
        public MappingConfiguration()
        {
            _typeConverter = new DefaultTypeConverter();
            _typeDefinitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            MapperFactory = new MapperFactory(_typeConverter, new PocoDataFactory(_typeDefinitions));
            StatementFactory = new StatementFactory();
        }

        /// <summary>
        /// Configures CqlPoco to use the specified type conversion factory when getting type conversion functions for converting 
        /// between data types in the database and your POCO objects.
        /// </summary>
        public MappingConfiguration ConvertTypesUsing(TypeConverter typeConverter)
        {
            if (typeConverter == null) throw new ArgumentNullException("typeConverter");
            _typeConverter = typeConverter;
            return this;
        }

        /// <summary>
        /// Specifies an individual mapping definition.  Usually used along with the <see cref="Map{TPoco}"/> class which
        /// allows you to define mappings with a fluent interface.  Will throw if a mapping has already been defined for a
        /// given POCO Type.
        /// </summary>
        public MappingConfiguration Define(params ITypeDefinition[] maps)
        {
            if (maps == null) return this;

            foreach (var typeDefinition in maps)
            {
                _typeDefinitions.Add(typeDefinition);
            }
            return this;
        }

        /// <summary>
        /// Specifies collections of <see cref="Mappings"/> specified.  Users should sub-class the <see cref="Mappings"/>
        /// class and use the fluent interface there to define mappings for POCOs.
        /// </summary>
        public MappingConfiguration Define(params Mappings[] mappings)
        {
            if (mappings == null) return this;

            foreach (var mapping in mappings)
            {
                foreach (var typeDefinition in mapping.Definitions)
                {
                    _typeDefinitions.Add(typeDefinition);
                }
            }
            return this;
        }

        /// <summary>
        /// Specifies a collection of mappings defined in Type T.  Type T should be a sub-class of <see cref="Mappings"/> and
        /// must have a parameter-less constructor.
        /// </summary>
        public MappingConfiguration Define<T>()
            where T : Mappings, new()
        {
            var mappings = new T();
            foreach (var map in mappings.Definitions)
            {
                _typeDefinitions.Add(map);
            }
            return this;
        }

        /// <summary>
        /// Clears all the mapping defined for this instance
        /// </summary>
        internal void Clear()
        {
            _typeDefinitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            MapperFactory = new MapperFactory(_typeConverter, new PocoDataFactory(_typeDefinitions));
            StatementFactory = new StatementFactory();
        }
    }
}
