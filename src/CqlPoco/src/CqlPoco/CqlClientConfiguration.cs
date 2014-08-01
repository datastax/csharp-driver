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
        private TypeConverterFactory _typeConverterFactory;

        private CqlClientConfiguration(ISession session)
        {
            if (session == null) throw new ArgumentNullException("session");
            _session = session;
            _typeConverterFactory = new DefaultTypeConverterFactory();
        }

        /// <summary>
        /// Configures CqlPoco to use the specified type conversion factory when getting type conversion functions for converting 
        /// between data types in the database and your POCO objects.
        /// </summary>
        public CqlClientConfiguration ConvertTypesUsing(TypeConverterFactory typeConverterFactory)
        {
            if (typeConverterFactory == null) throw new ArgumentNullException("typeConverterFactory");
            _typeConverterFactory = typeConverterFactory;
            return this;
        }

        /// <summary>
        /// Builds a ICqlClient using the configuration you've defined via the configuration interface.
        /// </summary>
        public ICqlClient BuildCqlClient()
        {
            var pocoDataFactory = new PocoDataFactory();
            return new CqlClient(_session, new MapperFactory(_typeConverterFactory, pocoDataFactory), new StatementFactory(pocoDataFactory));
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