//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Dse.Tasks;

namespace Dse.Data.Linq
{
    public class CqlQuerySingleElement<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlQuerySingleElement(Expression expression, CqlQuery<TEntity> source)
            : base(expression, source.Table, source.MapperFactory, source.StatementFactory, source.PocoData)
        {
            
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            return visitor.GetSelect(Expression, out values);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }

        public new CqlQuerySingleElement<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlQuerySingleElement<TEntity> SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        public new async Task<TEntity> ExecuteAsync()
        {
            var rs = await base.ExecuteAsync().ConfigureAwait(false);
            return rs.FirstOrDefault();
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

        /// <summary>
        /// Evaluates the Linq query, executes the cql statement and returns the first result.
        /// </summary>
        public new TEntity Execute()
        {
            var queryAbortTimeout = GetTable().GetSession().GetConfiguration()?.ClientOptions.QueryAbortTimeout ?? ClientOptions.DefaultQueryAbortTimeout;
            var task = ExecuteAsync();
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }
    }
}