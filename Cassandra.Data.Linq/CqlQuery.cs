using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data.Linq
{

    public class CqlQuerySingleElement<TEntity>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlQuerySingleElement(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public Type ElementType
        {
            get { return typeof (TEntity); }
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public override string ToString()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            return eval.Query;
        }

        public QueryTrace QueryTrace { get; private set; }

        public TEntity Execute()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                QueryTrace = outp.QueryTrace;

                if (outp.RowsCount == 0)
                    if (((MethodCallExpression) Expression).Method.Name == "First")
                        throw new InvalidOperationException("Sequence contains no elements.");
                    else if (((MethodCallExpression) Expression).Method.Name == "FirstOrDefault")
                        return default (TEntity);

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);

                return CqlQueryTools.GetRowFromCqlRow<TEntity>(outp.GetRows().First(), colToIdx, alter);
            }
        }

        private struct CqlQueryTag
        {
            public Context Context;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            return conn.BeginExecuteReadQuery(cqlQuery, callback, state,
                                              new CqlQueryTag() {AlternativeMapping = alter, Context = conn});
        }

        public TEntity EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var conn = tag.Context;
            using (var outp = conn.EndExecuteReadQuery(ar))
            {
                QueryTrace = outp.QueryTrace;

                if (outp.RowsCount == 0)
                    if (((MethodCallExpression) Expression).Method.Name == "First")
                        throw new InvalidOperationException("Sequence contains no elements.");
                    else if (((MethodCallExpression) Expression).Method.Name == "FirstOrDefault")
                        return default(TEntity);

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);

                return CqlQueryTools.GetRowFromCqlRow<TEntity>(outp.GetRows().First(), colToIdx, tag.AlternativeMapping);
            }
        }
    }


    public class CqlScalar<T>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }

        internal CqlScalar(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public T Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.CountQuery;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                QueryTrace = outp.QueryTrace;

                if (outp.RowsCount != 1)
                    throw new InvalidOperationException();

                var cols = outp.Columns;
                if (cols.Length != 1)
                    throw new InvalidOperationException();
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    return (T) row[0];
                }
            }

            throw new InvalidOperationException();
        }

        private struct CqlQueryTag
        {
            public Context Context;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            return conn.BeginExecuteReadQuery(cqlQuery, callback, state,
                                              new CqlQueryTag() {AlternativeMapping = alter, Context = conn});
        }

        public T EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var conn = tag.Context;
            using (var outp = conn.EndExecuteReadQuery(ar))
            {
                QueryTrace = outp.QueryTrace;

                if (outp.RowsCount != 1)
                    throw new InvalidOperationException();

                var cols = outp.Columns;
                if (cols.Length != 1)
                    throw new InvalidOperationException();
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    return (T) row[0];
                }
            }

            throw new InvalidOperationException();
        }
    }


    public class CqlQuery<TEntity> : IQueryable, IQueryable<TEntity>, IOrderedQueryable
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }

        internal CqlQuery()
        {
            this._expression = Expression.Constant(this);
            this._table = (Table<TEntity>) this;
        }

        internal CqlQuery(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new InvalidOperationException("Did you forget to Execute()?");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof (TEntity); }

        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public IQueryProvider Provider
        {
            get { return _table; }
        }

        public override string ToString()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            return eval.Query;
        }

        public IEnumerable<TEntity> Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                QueryTrace = outp.QueryTrace;

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, alter);
                }
            }
        }

        private struct CqlQueryTag
        {
            public Context Context;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            return conn.BeginExecuteReadQuery(cqlQuery, callback, state,
                                              new CqlQueryTag() {AlternativeMapping = alter, Context = conn});
        }

        public IEnumerable<TEntity> EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var conn = tag.Context;
            using (var outp = conn.EndExecuteReadQuery(ar))
            {
                QueryTrace = outp.QueryTrace;

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, tag.AlternativeMapping);
                }
            }
        }
    }

    public interface ICqlCommand
    {
        ITable GetTable();
        string GetCql();
        void Execute();
        IAsyncResult BeginExecute(AsyncCallback callback, object state);
        void EndExecute(IAsyncResult ar);
    }

    public class CqlDelete : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }

        internal CqlDelete(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public ITable GetTable()
        {
            return (_table as ITable);
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        public override string ToString()
        {
            return GetCql();
        }

        public void Execute()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            var conn = (_table as ITable).GetContext();
            var res = conn.ExecuteWriteQuery(cqlQuery);
            QueryTrace = res.QueryTrace;
        }

        private struct CqlQueryTag
        {
            public Context Context;
        }
        
        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            var conn = (_table as ITable).GetContext();
            return conn.BeginExecuteWriteQuery(cqlQuery, callback, state,
                                              new CqlQueryTag() { Context = conn });
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var conn = tag.Context;
            var res = conn.EndExecuteWriteQuery(ar);
            QueryTrace = res.QueryTrace;
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            return cqlQuery;
        }

    }

    public class CqlUpdate : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }

        internal CqlUpdate(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        public ITable GetTable()
        {
            return (_table as ITable);
        }

        public override string ToString()
        {
            return GetCql();
        }

        public void Execute()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            var conn = (_table as ITable).GetContext();
            var res = conn.ExecuteWriteQuery(cqlQuery);
            QueryTrace = res.QueryTrace;
        }
        
        private struct CqlQueryTag
        {
            public Context Context;
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            var conn = (_table as ITable).GetContext();
            return conn.BeginExecuteWriteQuery(cqlQuery, callback, state,
                                              new CqlQueryTag() { Context = conn });
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var conn = tag.Context;
            var res = conn.EndExecuteWriteQuery(ar);
            QueryTrace = res.QueryTrace;
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            return cqlQuery;
        }
    }
}
