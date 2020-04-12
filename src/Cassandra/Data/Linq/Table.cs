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

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// A Linq IQueryProvider that represents a table in Cassandra
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class Table<TEntity> : CqlQuery<TEntity>, ITable
    {
        private readonly ISession _session;
        private readonly string _name;
        private readonly string _keyspaceName;

        /// <summary>
        /// Gets the name of the Table in Cassandra
        /// </summary>
        public string Name
        {
            get { return _name ?? PocoData.TableName; }
        }

        /// <summary>
        /// Gets the name of the keyspace used. If null, it uses the active session keyspace.
        /// </summary>
        public string KeyspaceName
        {
            get { return _keyspaceName ?? PocoData.KeyspaceName; }
        }

        /// <summary>
        /// <para>Creates a new instance of the Linq IQueryProvider that represents a table in Cassandra using the mapping configuration provided.</para>
        /// <para>Use this constructor if you want to use a different table and keyspace names than the ones defined in the mapping configuration.</para>
        /// <para>Fluent configuration or attributes can be used to define mapping information.</para>
        /// </summary>
        /// <remarks>
        /// In case no mapping information is defined, case-insensitive class and method names will be used.
        /// </remarks>
        /// <param name="session">Session instance to be used to execute the statements</param>
        /// <param name="config">Mapping configuration</param>
        /// <param name="tableName">Name of the table</param>
        /// <param name="keyspaceName">Name of the keyspace were the table was created.</param>
        public Table(ISession session, MappingConfiguration config, string tableName, string keyspaceName)
        {
            _session = session;
            _name = tableName;
            _keyspaceName = keyspaceName;
            //In case no mapping has been defined for the type, determine if the attributes used are Linq or Cassandra.Mapping
            //Linq attributes are marked as Obsolete
            #pragma warning disable 612
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TEntity),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(TEntity)));
            #pragma warning restore 612
            var pocoData = config.MapperFactory.GetPocoData<TEntity>();
            InternalInitialize(Expression.Constant(this), this, config.MapperFactory, config.StatementFactory, pocoData);
        }

        /// <summary>
        /// <para>Creates a new instance of the Linq IQueryProvider that represents a table in Cassandra using the mapping configuration provided.</para>
        /// <para>Use this constructor if you want to use a different table name than the one defined in the mapping configuration.</para>
        /// <para>Fluent configuration or attributes can be used to define mapping information.</para>
        /// </summary>
        /// <remarks>
        /// In case no mapping information is defined, case-insensitive class and method names will be used.
        /// </remarks>
        /// <param name="session">Session instance to be used to execute the statements</param>
        /// <param name="config">Mapping configuration</param>
        /// <param name="tableName">Name of the table</param>
        public Table(ISession session, MappingConfiguration config, string tableName)
            : this(session, config, tableName, null)
        {

        }

        /// <summary>
        /// <para>Creates a new instance of the Linq IQueryProvider that represents a table in Cassandra using the mapping configuration provided.</para>
        /// <para>Fluent configuration or attributes can be used to define mapping information.</para>
        /// </summary>
        /// <remarks>
        /// In case no mapping information is defined, case-insensitive class and method names will be used.
        /// </remarks>
        /// <param name="session">Session instance to be used to execute the statements</param>
        /// <param name="config">Mapping configuration</param>
        public Table(ISession session, MappingConfiguration config)
            : this(session, config, null, null)
        {

        }

        /// <summary>
        /// Creates a new instance of the Linq IQueryProvider that represents a table in Cassandra using <see cref="MappingConfiguration.Global"/> configuration.
        /// </summary>
        /// <param name="session">Session instance to be used to execute the statements</param>
        public Table(ISession session) : this(session, MappingConfiguration.Global)
        {
            _session = session;
        }

        /// <summary>
        /// Creates a <see cref="CqlQuery&lt;T&gt;"/>
        /// </summary>
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new CqlQuery<TElement>(expression, this, MapperFactory, StatementFactory, PocoData);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            //Implementation of IQueryProvider
            throw new NotImplementedException();
        }

        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            //Implementation of IQueryProvider
            throw new NotImplementedException();
        }

        object IQueryProvider.Execute(Expression expression)
        {
            //Implementation of IQueryProvider
            throw new NotImplementedException();
        }

        public Type GetEntityType()
        {
            return typeof (TEntity);
        }

        public void Create()
        {
            var serializer = _session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var cqlQueries = CqlGenerator.GetCreate(serializer, PocoData, Name, KeyspaceName, false);
            foreach (var cql in cqlQueries)
            {
                _session.Execute(cql);
            }
        }

        public void CreateIfNotExists()
        {
            try
            {
                Create();
            }
            catch (AlreadyExistsException)
            {
                //do nothing
            }
        }

        public async Task CreateAsync()
        {
            var serializer = _session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var cqlQueries = CqlGenerator.GetCreate(serializer, PocoData, Name, KeyspaceName, false);
            foreach (var cql in cqlQueries)
            {
                await _session.ExecuteAsync(new SimpleStatement(cql)).ConfigureAwait(false);
            }
        }

        public async Task CreateIfNotExistsAsync()
        {
            try
            {
                await CreateAsync().ConfigureAwait(false);
            }
            catch (AlreadyExistsException)
            {
                //do nothing
            }
        }

        public ISession GetSession()
        {
            return _session;
        }

        public TableType GetTableType()
        {
            return PocoData.Columns.Any(c => c.IsCounter) ? TableType.Counter : TableType.Standard;
        }

        /// <summary>
        /// Returns a new <see cref="CqlInsert{TEntity}"/> command. Use
        /// <see cref="CqlCommand.Execute()"/> method to execute the query.
        /// </summary>
        public CqlInsert<TEntity> Insert(TEntity entity)
        {
            return Insert(entity, true);
        }

        /// <summary>
        /// Returns a new <see cref="CqlInsert{TEntity}"/> command. Use
        /// <see cref="CqlCommand.Execute()"/> method to execute the query.
        /// </summary>
        /// <param name="entity">The entity to insert</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> 
        /// entity members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        public CqlInsert<TEntity> Insert(TEntity entity, bool insertNulls)
        {
            return new CqlInsert<TEntity>(entity, insertNulls, this, StatementFactory, MapperFactory);
        }
    }
}
