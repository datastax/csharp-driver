using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Cassandra.Data.Linq
{
    public abstract class CqlQueryBase<TEntity> : Statement
    {
        private Expression _expression;
        private IQueryProvider _table;

        public Expression Expression
        {
            get { return _expression; }
        }

        public Type ElementType
        {
            get { return typeof (TEntity); }
        }

        public QueryTrace QueryTrace { get; protected set; }

        public override RoutingKey RoutingKey
        {
            get { return null; }
        }

        internal CqlQueryBase()
        {
        }

        internal CqlQueryBase(Expression expression, IQueryProvider table)
        {
            _expression = expression;
            _table = table;
        }

        internal void InternalInitialize(Expression expression, IQueryProvider table)
        {
            _expression = expression;
            _table = table;
        }

        public ITable GetTable()
        {
            return _table as ITable;
        }

        protected abstract string GetCql(out object[] values);

        protected Task<RowSet> InternalExecuteAsync(string cqlQuery, object[] values)
        {
            var session = GetTable().GetSession();
            SimpleStatement stmt = new SimpleStatement(cqlQuery).BindObjects(values);
            this.CopyQueryPropertiesTo(stmt);
            return session.ExecuteAsync(stmt);
        }

        /// <summary>
        /// Evaluates the Linq query, executes asynchronously the cql statement and adapts the results.
        /// </summary>
        public Task<IEnumerable<TEntity>> ExecuteAsync()
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;

            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] values;
            string cql = visitor.GetSelect(out values, withValues);
            var adaptation =
                InternalExecuteAsync(cql, values).ContinueWith((t) =>
                {
                    var rs = t.Result;
                    QueryTrace = rs.Info.QueryTrace;

                    CqlColumn[] cols = rs.Columns;
                    var colToIdx = new Dictionary<string, int>();
                    for (int idx = 0; idx < cols.Length; idx++)
                        colToIdx.Add(cols[idx].Name, idx);
                    return AdaptRows(rs, colToIdx, visitor);
                }, TaskContinuationOptions.ExecuteSynchronously);
            return adaptation;
        }

        internal IEnumerable<TEntity> AdaptRows(IEnumerable<Row> rows, Dictionary<string, int> colToIdx, CqlExpressionVisitor visitor)
        {
            foreach (Row row in rows)
            {
                yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, visitor.Mappings, visitor.Alter);
            }
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

        protected struct CqlQueryTag
        {
            public Dictionary<string, string> Alter;
            public Dictionary<string, Tuple<string, object, int>> Mappings;
            public ISession Session;
        }
    }
}