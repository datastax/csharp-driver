//
//      Copyright (C) 2012-2017 DataStax Inc.
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
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    public abstract class CqlQueryBase<TEntity> : Statement
    {
        private QueryTrace _queryTrace;
        internal ITable Table { get; private set; }

        public Expression Expression { get; private set; }

        public Type ElementType
        {
            get { return typeof (TEntity); }
        }

        /// <summary>
        /// After being executed, it retrieves the trace of the CQL query.
        /// <para>Use <see cref="IStatement.EnableTracing"/> to enable tracing.</para>
        /// <para>
        /// Note that enabling query trace introduces server-side overhead by storing request information, so it's
        /// recommended that you only enable query tracing when trying to identify possible issues / debugging. 
        /// </para>
        /// </summary>
        public QueryTrace QueryTrace
        {
            get => Volatile.Read(ref _queryTrace);
            protected set => Volatile.Write(ref _queryTrace, value);
        }

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

        internal CqlQueryBase(Expression expression, ITable table, MapperFactory mapperFactory, StatementFactory stmtFactory, PocoData pocoData)
        {
            InternalInitialize(expression, table, mapperFactory, stmtFactory, pocoData);
        }

        internal void InternalInitialize(Expression expression, ITable table, MapperFactory mapperFactory, StatementFactory stmtFactory, PocoData pocoData)
        {
            Expression = expression;
            Table = table;
            MapperFactory = mapperFactory;
            StatementFactory = stmtFactory;
            PocoData = pocoData;
        }

        public ITable GetTable()
        {
            return Table;
        }

        protected abstract string GetCql(out object[] values);
        
        protected async Task<RowSet> InternalExecuteWithProfileAsync(string executionProfile, string cqlQuery, object[] values)
        {
            var session = GetTable().GetSession();
            var statement = await StatementFactory.GetStatementAsync(session, Cql.New(cqlQuery, values))
                                                  .ConfigureAwait(false);
            
            this.CopyQueryPropertiesTo(statement);
            var rs = executionProfile != null
                ? await session.ExecuteAsync(statement, executionProfile).ConfigureAwait(false)
                : await session.ExecuteAsync(statement).ConfigureAwait(false);
            QueryTrace = rs.Info.QueryTrace;
            return rs;
        }

        /// <summary>
        /// Projects a RowSet that is the result of a given cql query into a IEnumerable{TEntity}.
        /// </summary>
        internal virtual IEnumerable<TEntity> AdaptResult(string cql, RowSet rs)
        {
            var mapper = MapperFactory.GetMapper<TEntity>(cql, rs);
            return rs.Select(mapper);
        }

        /// <summary>
        /// Evaluates the Linq query, executes asynchronously the cql statement and adapts the results.
        /// </summary>
        public Task<IEnumerable<TEntity>> ExecuteAsync()
        {
            return ExecuteCqlQueryAsync(null);
        }

        /// <summary>
        /// Evaluates the Linq query, executes the cql statement and adapts the results.
        /// </summary>
        public IEnumerable<TEntity> Execute()
        {
            return ExecuteCqlQuery(null);
        }
        
        /// <summary>
        /// Evaluates the Linq query, executes asynchronously the cql statement with the provided execution profile
        /// and adapts the results.
        /// </summary>
        public Task<IEnumerable<TEntity>> ExecuteAsync(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecuteCqlQueryAsync(executionProfile);
        }

        /// <summary>
        /// Evaluates the Linq query, executes the cql statement with the provided execution profile and adapts the results.
        /// </summary>
        public IEnumerable<TEntity> Execute(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecuteCqlQuery(executionProfile);
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
        
        private IEnumerable<TEntity> ExecuteCqlQuery(string executionProfile)
        {
            var queryAbortTimeout = GetTable().GetSession().Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
            var task = ExecuteCqlQueryAsync(executionProfile);
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }
        
        private async Task<IEnumerable<TEntity>> ExecuteCqlQueryAsync(string executionProfile)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            object[] values;
            var cql = visitor.GetSelect(Expression, out values);
            var rs = await InternalExecuteWithProfileAsync(executionProfile, cql, values).ConfigureAwait(false);
            return AdaptResult(cql, rs);
        }
    }
}