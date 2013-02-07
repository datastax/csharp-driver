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
        public bool QueryTraceingEnabled { get; private set; }

        private struct CqlQueryTag
        {
            public Session Session;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute( ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { AlternativeMapping = alter, Session = ctx }, callback, state);
        }

        public TEntity EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            using (var outp = ctx.EndExecute(ar))
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

        public TEntity Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            return EndExecute(BeginExecute(consistencyLevel, null, null));
        }

    }


    public class CqlScalar<T>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }
        public bool QueryTraceingEnabled { get; private set; }

        internal CqlScalar(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public T Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            return EndExecute(BeginExecute(consistencyLevel, null, null));
        }

        private struct CqlQueryTag
        {
            public Session Session;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.CountQuery;
            var alter = eval.AlternativeMapping;
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { AlternativeMapping = alter, Session = ctx }, callback, state);
        }

        public T EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            using (var outp = ctx.EndExecute(ar))
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
        public bool QueryTraceingEnabled { get; private set; }

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

        public IEnumerable<TEntity> Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            return EndExecute(BeginExecute(consistencyLevel, null, null));
        }

        private struct CqlQueryTag
        {
            public Session Session;
            public Dictionary<string, string> AlternativeMapping;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { AlternativeMapping = alter, Session = ctx }, callback, state);
        }

        public IEnumerable<TEntity> EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            using (var outp = ctx.EndExecute(ar))
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
        string GetCql();
        bool IsQueryTraceEnabled();
        void SetQueryTrace(QueryTrace trace);
        ITable GetTable();
        void Execute(ConsistencyLevel consistencyLevel);
    }

    public class CqlDelete : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }
        public bool QueryTraceingEnabled { get; private set; }

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

        public void Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            EndExecute(BeginExecute(consistencyLevel, null, null));
        }

        private struct CqlQueryTag
        {
            public Session Session;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { Session = ctx }, callback, state);
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var res = ctx.EndExecute(ar);
            QueryTrace = res.QueryTrace;
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            return cqlQuery;
        }

        public bool IsQueryTraceEnabled()
        {
            return QueryTraceingEnabled;
        }

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }
    }

    public class CqlInsert<TEntity> : ICqlCommand
    {
        private readonly TEntity _entity;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }
        public bool QueryTraceingEnabled { get; private set; }

        internal CqlInsert(TEntity entity, IQueryProvider table)
        {
            this._entity = entity;
            this._table = table;
        }

        public ITable GetTable()
        {
            return (_table as ITable);
        }

        public override string ToString()
        {
            return GetCql();
        }

        public void Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            EndExecute(BeginExecute(consistencyLevel, null, null));
        }
        
        private struct CqlQueryTag
        {
            public Session Session;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var cqlQuery = GetCql();
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { Session = ctx }, callback, state);
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var res = ctx.EndExecute(ar);
            QueryTrace = res.QueryTrace;
        }

        public string GetCql()
        {
            return CqlQueryTools.GetInsertCQL(_entity, (_table as ITable).GetTableName());
        }

        public bool IsQueryTraceEnabled()
        {
            return QueryTraceingEnabled;
        }

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }
    }

    public class CqlUpdate : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public QueryTrace QueryTrace { get; private set; }
        public bool QueryTraceingEnabled { get; private set; }

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

        public void Execute(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            EndExecute(BeginExecute(consistencyLevel, null, null));
        }
        
        private struct CqlQueryTag
        {
            public Session Session;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            var ctx = (_table as ITable).GetSession();
            return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(QueryTraceingEnabled).SetConsistencyLevel(consistencyLevel),
                                new CqlQueryTag() { Session = ctx }, callback, state);
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var res = ctx.EndExecute(ar);
            QueryTrace = res.QueryTrace;
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            return cqlQuery;
        }

        public bool IsQueryTraceEnabled()
        {
            return QueryTraceingEnabled;
        }

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }
    }
}
