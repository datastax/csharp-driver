//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Mapping;

namespace Dse.Data.Linq
{
    public static class SessionExtensions
    {
        /// <summary>
        /// <para>Extension method used for backward-compatibility, use <see cref="Dse.Data.Linq.Table{T}(ISession)"/> constructor instead.</para>
        /// <para>Creates a new instance of the Linq IQueryProvider that represents a table in Cassandra using the mapping configuration provided.</para>
        /// <para>Fluent configuration or attributes can be used to define mapping information.</para>
        /// </summary>
        /// <remarks>
        /// In case no mapping information is defined, <strong>case-sensitive</strong> class and method names will be used.
        /// </remarks>
        /// <typeparam name="TEntity">The object type</typeparam>
        /// <param name="session">The session to be used to execute the statements</param>
        /// <param name="tableName">The table name in Cassandra. If null, it will be retrieved from the TEntity information.</param>
        /// <param name="keyspaceName">The keyspace in which the table exists. If null, the current session keyspace will be used.</param>
        /// <returns></returns>
        public static Table<TEntity> GetTable<TEntity>(this ISession session, string tableName = null, string keyspaceName = null)
        {
            //Use Linq defaults if no definition has been set for this types
            //Linq attributes are marked as Obsolete
            #pragma warning disable 612
            MappingConfiguration.Global.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TEntity),
                () => new LinqAttributeBasedTypeDefinition(typeof (TEntity), tableName, keyspaceName));
            #pragma warning restore 612
            var config = MappingConfiguration.Global;
            return new Table<TEntity>(session, config, tableName, keyspaceName);
        }

        public static Batch CreateBatch(this ISession session)
        {
            return CreateBatch(session, BatchType.Logged);
        } 

        public static Batch CreateBatch(this ISession session, BatchType batchType)
        {
            if (session == null || session.BinaryProtocolVersion > 1)
            {
                return new BatchV2(session, batchType);
            }
            return new BatchV1(session, batchType);
        }

        internal static Configuration GetConfiguration(this ISession session)
        {
            Configuration config = null;
            if (session is Session)
            {
                config = ((Session) session).Configuration;
            }
            else
            {
                //Get the default options
                config = new Configuration();
            }
            return config;
        }
    }
}
