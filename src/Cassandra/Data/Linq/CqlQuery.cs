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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents a Linq query that gets evaluated as a CQL statement.
    /// </summary>
    public class CqlQuery<TEntity> : CqlQueryBase<TEntity>, IQueryable<TEntity>, IOrderedQueryable
    {
        internal CqlQuery()
        {
        }

        internal CqlQuery(Expression expression, ITable table, MapperFactory mapperFactory, StatementFactory stmtFactory, PocoData pocoData)
            : base(expression, table, mapperFactory, stmtFactory, pocoData)
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// IQueryable.Provider implementation
        /// </summary>
        public IQueryProvider Provider
        {
            get { return GetTable(); }
        }

        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new InvalidOperationException("You must explicitly invoke ExecuteAsync() or Execute()");
        }

        public new CqlQuery<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlQuery<TEntity> SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        /// <summary>
        /// Sets the page size for this query.
        /// The page size controls how much resulting rows will be retrieved
        /// simultaneously (the goal being to avoid loading too much results
        /// in memory for queries yielding large results). Please note that
        /// while value as low as 1 can be used, it is highly discouraged to
        /// use such a low value in practice as it will yield very poor
        /// performance. If in doubt, leaving the default is probably a good
        /// idea.
        /// </summary>
        /// <returns>This instance</returns>
        public new CqlQuery<TEntity> SetPageSize(int pageSize)
        {
            base.SetPageSize(pageSize);
            return this;
        }

        /// <summary>
        /// Sets the paging state, a token representing the current page state of query used to continue paging by retrieving the following result page.
        /// Setting the paging state will disable automatic paging.
        /// </summary>
        /// <returns>This instance</returns>
        public new CqlQuery<TEntity> SetPagingState(byte[] pagingState)
        {
            base.SetPagingState(pagingState);
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            return visitor.GetSelect(Expression, out values);
        }

        /// <summary>
        /// Asynchronously executes the query and returns a task of a page of results
        /// </summary>
        public Task<IPage<TEntity>> ExecutePagedAsync()
        {
            return ExecutePagedWithProfileAsync(null);
        }

        /// <summary>
        /// Executes the query and returns a page of results
        /// </summary>
        public IPage<TEntity> ExecutePaged()
        {
            return ExecutePagedWithProfile(null);
        }
        
        /// <summary>
        /// Asynchronously executes the query with the provided execution profile and returns a task of a page of results
        /// </summary>
        public Task<IPage<TEntity>> ExecutePagedAsync(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecutePagedWithProfileAsync(executionProfile);
        }

        /// <summary>
        /// Executes the query with the provided execution profile and returns a page of results
        /// </summary>
        public IPage<TEntity> ExecutePaged(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecutePagedWithProfile(executionProfile);
        }

        private async Task<IPage<TEntity>> ExecutePagedWithProfileAsync(string executionProfile)
        {
            SetAutoPage(false);
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            object[] values;
            var cql = visitor.GetSelect(Expression, out values);
            var rs = await InternalExecuteWithProfileAsync(executionProfile, cql, values).ConfigureAwait(false);
            var mapper = MapperFactory.GetMapper<TEntity>(cql, rs);
            return new Page<TEntity>(rs.Select(mapper), PagingState, rs.PagingState);
        }

        private IPage<TEntity> ExecutePagedWithProfile(string executionProfile)
        {
            var queryAbortTimeout = GetTable().GetSession().Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
            var task = ExecutePagedWithProfileAsync(executionProfile);
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }

        /// <summary>
        /// Generates and returns cql query for this instance 
        /// </summary>
        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}
