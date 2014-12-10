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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Mapping.TypeConversion;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// A Linq IQueryProvider that represents a table in Cassandra
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class Table<TEntity> : CqlQuery<TEntity>, ITable
    {
        private readonly ISession _session;

        /// <summary>
        /// Gets the name of the Table in Cassandra
        /// </summary>
        public string Name
        {
            get { return PocoData.TableName; }
        }

        internal Table(ISession session, MapperFactory mapperFactory, StatementFactory stmtFactory)
        {
            _session = session;
            var pocoData = mapperFactory.GetPocoData<TEntity>();
            InternalInitialize(Expression.Constant(this), this, mapperFactory, stmtFactory, pocoData);
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
            var cqlQueries = CqlGenerator.GetCreate(PocoData, false);
            foreach (var cql in cqlQueries)
            {
                _session.WaitForSchemaAgreement(_session.Execute(cql));
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

        public ISession GetSession()
        {
            return _session;
        }

        public TableType GetTableType()
        {
            return PocoData.Columns.Any(c => c.IsCounter) ? TableType.Counter : TableType.Standard;
        }

        /// <summary>
        /// Returns a CqlInsert command to be executed against a table. To execute this command, use Execute() method.
        /// </summary>
        public CqlInsert<TEntity> Insert(TEntity entity)
        {
            return new CqlInsert<TEntity>(entity, this, StatementFactory, MapperFactory);
        }
    }
}
