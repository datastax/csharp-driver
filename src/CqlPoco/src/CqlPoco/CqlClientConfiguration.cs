using System;
using Cassandra;
using CqlPoco.Mapping;
using CqlPoco.Statements;
using CqlPoco.TypeConversion;

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

        private CqlClientConfiguration(ISession session)
        {
            if (session == null) throw new ArgumentNullException("session");
            _session = session;
            _typeConverter = new DefaultTypeConverter();
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
        /// Builds a ICqlClient using the configuration you've defined via the configuration interface.
        /// </summary>
        public ICqlClient BuildCqlClient()
        {
            var pocoDataFactory = new PocoDataFactory();
            return new CqlClient(_session, new MapperFactory(_typeConverter, pocoDataFactory), new StatementFactory(),
                                 new CqlStringGenerator(pocoDataFactory));
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