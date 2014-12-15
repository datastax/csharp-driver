//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using Cassandra.Mapping;

namespace Cassandra.Data.Linq
{
    public static class SessionExtensions
    {
        /// <summary>
        /// <para>Extension method used for backward-compatibility, use <see cref="Table&lt;T&gt;(Cassandra.ISession)"/> constructor instead.</para>
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
            MappingConfiguration.Global.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TEntity),
                () => new LinqAttributeBasedTypeDefinition(typeof (TEntity), tableName, keyspaceName));
            var config = MappingConfiguration.Global;
            //For backwards compatibility support multiple table names for a given Entity
            if (tableName != null && config.MapperFactory.GetPocoData<TEntity>().TableName != tableName)
            {
                //There is a name mismatch between the ones stored and the one provided
                //Use an specific mapping configuration
                config = new MappingConfiguration()
                    .Define(new LinqAttributeBasedTypeDefinition(typeof (TEntity), tableName, keyspaceName));
            }
            return new Table<TEntity>(session, config);
        }

        public static Batch CreateBatch(this ISession session)
        {
            if (session == null || session.BinaryProtocolVersion > 1)
            {
                return new BatchV2(session);
            }
            return new BatchV1(session);
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
