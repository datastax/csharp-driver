using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Cassandra.Data.Linq
{
    public class CqlScalar<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlScalar(Expression expression, IQueryProvider table) : base(expression, table)
        {
        }

        public new TEntity Execute()
        {
            var config = GetTable().GetSession().GetConfiguration();
            var task = ExecuteAsync();
            return TaskHelper.WaitToComplete(task, config.ClientOptions.QueryAbortTimeout);
        }

        public new CqlScalar<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetCount(out values);
        }

        public override string ToString()
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] _;
            return visitor.GetCount(out _, false);
        }

        public new Task<TEntity> ExecuteAsync()
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;

            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);

            object[] values;
            string cql = visitor.GetCount(out values, withValues);

            Task<TEntity> adaptation =
                InternalExecuteAsync(cql, values).ContinueWith((t) =>
                {
                    var rs = t.Result;
                    var result = default(TEntity);
                    var row = rs.FirstOrDefault();
                    if (row != null)
                    {
                        result = (TEntity)row[0];
                    }
                    return result;
                }, TaskContinuationOptions.ExecuteSynchronously);
            return adaptation;
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