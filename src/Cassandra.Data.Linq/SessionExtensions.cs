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
using Cassandra.Mapping.Statements;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Mapping.Utils;

namespace Cassandra.Data.Linq
{
    public static class SessionExtensions
    {
        /// <summary>
        /// Creates a Linq IQueryProvider based on the type provided
        /// </summary>
        /// <typeparam name="TEntity">The object type</typeparam>
        /// <param name="session"></param>
        /// <param name="tableName">The table name in Cassandra. If null, it will be retrieved from the TEntity information.</param>
        /// <param name="keyspaceName">The keyspace in which the table exists. If null, the current session keyspace will be used.</param>
        /// <returns></returns>
        public static Table<TEntity> GetTable<TEntity>(this ISession session, string tableName = null, string keyspaceName = null)
        {
            var mappingInformation = new LinqAttributeBasedTypeDefinition(typeof (TEntity), tableName, keyspaceName);
            return GetTable<TEntity>(session, mappingInformation);
        }

        /// <summary>
        /// Creates a Linq IQueryProvider based on the type and mapping information provided
        /// </summary>
        /// <typeparam name="TEntity">The object type</typeparam>
        /// <param name="session"></param>
        /// <param name="mappingInformation">Instance of ITypeDefinition to obtain the mapping information.</param>
        /// <returns></returns>
        public static Table<TEntity> GetTable<TEntity>(this ISession session, ITypeDefinition mappingInformation)
        {
            if (mappingInformation == null)
            {
                throw new ArgumentNullException("mappingInformation");
            }
            var definitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType)
            {
                mappingInformation
            };
            var pocoDataFactory = new PocoDataFactory(definitions);
            var mapperFactory = new MapperFactory(new DefaultTypeConverter(), pocoDataFactory);
            var statementFactory = new StatementFactory(session);
            return new Table<TEntity>(session, mapperFactory, statementFactory);
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
