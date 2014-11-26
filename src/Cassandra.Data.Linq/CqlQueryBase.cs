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
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Mapping;
using Cassandra.Mapping.Statements;

namespace Cassandra.Data.Linq
{
    public abstract class CqlQueryBase<TEntity> : Statement
    {
        private IQueryProvider _table;

        public Expression Expression { get; private set; }

        public Type ElementType
        {
            get { return typeof (TEntity); }
        }

        public QueryTrace QueryTrace { get; protected set; }

        internal MapperFactory MapperFactory { get; set; }

        internal StatementFactory StatementFactory { get; set; }
        /// <summary>
        /// The information associated with the TEntity
        /// </summary>
        internal PocoData PocoData { get; set; }

        public override RoutingKey RoutingKey
        {
            get { return null; }
        }

        internal CqlQueryBase()
        {
        }

        internal CqlQueryBase(Expression expression, IQueryProvider table, MapperFactory mapperFactory, StatementFactory stmtFactory, PocoData pocoData)
        {
            InternalInitialize(expression, table, mapperFactory, stmtFactory, pocoData);
        }

        internal void InternalInitialize(Expression expression, IQueryProvider table, MapperFactory mapperFactory, StatementFactory stmtFactory, PocoData pocoData)
        {
            Expression = expression;
            _table = table;
            MapperFactory = mapperFactory;
            StatementFactory = stmtFactory;
            PocoData = pocoData;
        }

        public ITable GetTable()
        {
            return _table as ITable;
        }

        protected abstract string GetCql(out object[] values);

        protected Task<RowSet> InternalExecuteAsync(string cqlQuery, object[] values)
        {
            var session = GetTable().GetSession();
            return StatementFactory
                .GetStatementAsync(Cql.New(cqlQuery, values))
                .Continue(t1 =>
                {
                    var stmt = t1.Result;
                    this.CopyQueryPropertiesTo(stmt);
                    return session.ExecuteAsync(stmt);
                }).Unwrap();
        }

        /// <summary>
        /// Evaluates the Linq query, executes asynchronously the cql statement and adapts the results.
        /// </summary>
        public Task<IEnumerable<TEntity>> ExecuteAsync()
        {
            var visitor = new CqlExpressionVisitor(MapperFactory.GetPocoData<TEntity>());
            visitor.Evaluate(Expression);
            object[] values;
            var cql = visitor.GetSelect(out values);
            var adaptation = InternalExecuteAsync(cql, values).Continue(t =>
            {
                var rs = t.Result;
                var mapper = MapperFactory.GetMapper<TEntity>(cql, rs);
                return rs.Select(mapper);
            });
            return adaptation;
        }

        /// <summary>
        /// Evaluates the Linq query, executes the cql statement and adapts the results.
        /// </summary>
        public IEnumerable<TEntity> Execute()
        {
            var config = GetTable().GetSession().GetConfiguration();
            var task = ExecuteAsync();
            return TaskHelper.WaitToComplete(task, config.ClientOptions.QueryAbortTimeout);
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return ExecuteAsync().ToApm(callback, state);
        }

        public IEnumerable<TEntity> EndExecute(IAsyncResult ar)
        {
            var task = (Task<IEnumerable<TEntity>>)ar;
            return task.Result;
        }
    }
}