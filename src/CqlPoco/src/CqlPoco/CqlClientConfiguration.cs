using System;
using Cassandra;
using CqlPoco.FluentMapping;
using CqlPoco.Mapping;
using CqlPoco.Statements;
using CqlPoco.TypeConversion;
using CqlPoco.Utils;

namespace CqlPoco
{
    /// <summary>
    /// Main entry point for configuration CqlPoco and retrieving an instance of ICqlClient.  Use the <see cref="ForSession"/> method
    /// to start configuring.
    /// </summary>
    public class CqlClientConfiguration
    {
        private readonly ISession _session;
        private TypeConverter _typeConverter;
        private readonly LookupKeyedCollection<Type, ITypeDefinition> _typeDefinitions; 

        private CqlClientConfiguration(ISession session)
        {
            if (session == null) throw new ArgumentNullException("session");
            _session = session;
            _typeConverter = new DefaultTypeConverter();
            _typeDefinitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
        }

        /// <summary>
        /// Configures CqlPoco to use the specified type conversion factory when getting type conversion functions for converting 
        /// between data types in the database and your POCO objects.
        /// </summary>
        public CqlClientConfiguration ConvertTypesUsing(TypeConverter typeConverter)
        {
            if (typeConverter == null) throw new ArgumentNullException("typeConverter");
            _typeConverter = typeConverter;
            return this;
        }

        /// <summary>
        /// Configures CqlPoco to use the collections of <see cref="Mappings"/> specified.  Users should sub-class the <see cref="Mappings"/>
        /// class and use the fluent interface there to define mappings for POCOs.
        /// </summary>
        public CqlClientConfiguration UseMappings(params Mappings[] mappings)
        {
            if (mappings == null) return this;

            foreach (Mappings mapping in mappings)
            {
                foreach (ITypeDefinition typeDefinition in mapping.Definitions)
                {
                    _typeDefinitions.Add(typeDefinition);
                }
            }
            return this;
        }

        /// <summary>
        /// Configures CqlPoco to use the collection of mappings defined in Type T.  Type T should be a sub-class of <see cref="Mappings"/> and
        /// must have a paramaterless constructor.
        /// </summary>
        public CqlClientConfiguration UseMappings<T>()
            where T : Mappings, new()
        {
            var mappings = new T();
            foreach (ITypeDefinition map in mappings.Definitions)
            {
                _typeDefinitions.Add(map);
            }
            return this;
        }

        /// <summary>
        /// Configures CqlPoco to use the individual mappings specified.  Usually used along with the <see cref="Map{TPoco}"/> class which
        /// allows you to define mappings with a fluent interface.  Will throw if a mapping has already been defined for a
        /// given POCO Type.
        /// </summary>
        public CqlClientConfiguration UseIndividualMappings(params ITypeDefinition[] maps)
        {
            if (maps == null) return this;

            foreach (ITypeDefinition typeDefinition in maps)
            {
                _typeDefinitions.Add(typeDefinition);
            }
            return this;
        }

        /// <summary>
        /// Configures CqlPoco to use the individual mapping specified by Type T.  Usually Type T will be a sub-class of <see cref="Map{TPoco}"/>
        /// and must have a parameterless constructor.
        /// </summary>
        public CqlClientConfiguration UseIndividualMapping<T>()
            where T : ITypeDefinition, new()
        {
            _typeDefinitions.Add(new T());
            return this;
        }

        /// <summary>
        /// Builds a ICqlClient using the configuration you've defined via the configuration interface.
        /// </summary>
        public ICqlClient BuildCqlClient()
        {
            var pocoDataFactory = new PocoDataFactory();
            return new CqlClient(_session, new MapperFactory(_typeConverter, pocoDataFactory), new StatementFactory(_session),
                                 new CqlGenerator(pocoDataFactory));
        }

        /// <summary>
        /// Starts building a CqlClientConfiguration for a given Cassandra driver session.  Be sure to call <see cref="BuildCqlClient"/>
        /// when you're finished to get an instance of the client for use in your application.
        /// </summary>
        /// <param name="session">The Cassandra driver session for interacting with the database.</param>
        /// <returns>A CqlClientConfiguration object that can be further configured.</returns>
        public static CqlClientConfiguration ForSession(ISession session)
        {
            return new CqlClientConfiguration(session);
        }
    }
}