//
//      Copyright (C) DataStax Inc.
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

using Cassandra.Mapping;

namespace Cassandra.Data.Linq
{
    public static class SessionExtensions
    {
        /// <summary> 
        /// <para>Deprecated. Extension method used for backward-compatibility, use <see cref="Cassandra.Data.Linq.Table{T}(Cassandra.ISession)"/> constructor instead.</para>
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
    }
}
