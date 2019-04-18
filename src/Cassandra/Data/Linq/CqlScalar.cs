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
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents an IQueryable that returns the first column of the first rows
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class CqlScalar<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlScalar(Expression expression, ITable table, StatementFactory stmtFactory, PocoData pocoData)
            : base(expression, table, null, stmtFactory, pocoData)
        {

        }

        public new TEntity Execute()
        {
            var queryAbortTimeout = GetTable().GetSession().Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
            var task = ExecuteAsync();
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }
        
        public new TEntity Execute(string executionProfile)
        {
            var queryAbortTimeout = GetTable().GetSession().Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
            var task = ExecuteAsync(executionProfile);
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }

        public new CqlScalar<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            return visitor.GetCount(Expression, out values);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }

        public new Task<TEntity> ExecuteAsync()
        {
            return ExecuteWithProfileAsync(null);
        }
        
        public new Task<TEntity> ExecuteAsync(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecuteWithProfileAsync(executionProfile);
        }

        private async Task<TEntity> ExecuteWithProfileAsync(string executionProfile)
        {
            object[] values;
            string cql = GetCql(out values);
            var rs = await InternalExecuteWithProfileAsync(executionProfile, cql, values).ConfigureAwait(false);
            var result = default(TEntity);
            var row = rs.FirstOrDefault();
            if (row != null)
            {
                result = (TEntity)row[0];
            }

            return result;
        }

        public new IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return ExecuteAsync().ToApm(callback, state);
        }

        public new TEntity EndExecute(IAsyncResult ar)
        {
            var task = (Task<TEntity>)ar;
            return task.Result;
        }
    }
}