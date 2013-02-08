using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data.Linq
{

    public abstract class CqlQueryBase<TEntity>  : Query
    {
        private Expression _expression;
        private IQueryProvider _table;

        internal CqlQueryBase()
        {
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        internal void InternalInitialize(Expression expression,IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        internal CqlQueryBase(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }
        
        public Type ElementType
        {
            get { return typeof(TEntity); }
        }

        public ITable GetTable() { return _table as ITable; }

        public abstract string CqlString();

        public override string ToString()
        {
            return CqlString();
        }

        public QueryTrace QueryTrace { get; protected set; }

        protected struct CqlQueryTag
        {
            public Session Session;
            public Dictionary<string, string> AlternativeMapping;
        }

        protected IAsyncResult InternalBeginExecute(string cqlQuery, Dictionary<string, string> alter, AsyncCallback callback, object state)
        {
            var session = GetTable().GetSession();

            return session.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(IsTracing).SetConsistencyLevel(ConsistencyLevel),
                                new CqlQueryTag() { AlternativeMapping = alter, Session = session }, callback, state);
        }

        protected CqlRowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var outp = ctx.EndExecute(ar);
            QueryTrace = outp.QueryTrace;
            return outp;
        }

        public abstract IAsyncResult BeginExecute(AsyncCallback callback, object state);

        protected override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return BeginExecute(callback, state);
        }

        protected override CqlRowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return InternalEndExecute(ar);
        }

        public override CassandraRoutingKey RoutingKey
        {
            get { return null; }
        }
    }

    public class CqlQuerySingleElement<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlQuerySingleElement(Expression expression, IQueryProvider table)
            : base(expression, table) { }


        public override string CqlString()
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return eval.Query;
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return InternalBeginExecute(eval.Query, eval.AlternativeMapping, callback, state);
        }

        public TEntity EndExecute(IAsyncResult ar)
        {
            using (var outp = InternalEndExecute(ar))
            {

                if (outp.RowsCount == 0)
                    if (((MethodCallExpression)Expression).Method.Name == "First")
                        throw new InvalidOperationException("Sequence contains no elements.");
                    else if (((MethodCallExpression)Expression).Method.Name == "FirstOrDefault")
                        return default(TEntity);

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);

                var tag = (CqlQueryTag)Session.GetTag(ar);
                return CqlQueryTools.GetRowFromCqlRow<TEntity>(outp.GetRows().First(), colToIdx, tag.AlternativeMapping);
            }
        }

        public TEntity Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }
    }


    public class CqlScalar<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlScalar(Expression expression, IQueryProvider table) :base(expression,table){}

        public TEntity Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }
        
        public override string CqlString()
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return eval.CountQuery;
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return InternalBeginExecute(eval.CountQuery,eval.AlternativeMapping,callback,state);
        }

        public TEntity EndExecute(IAsyncResult ar)
        {
            using (var outp = InternalEndExecute(ar))
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
                    return (TEntity)row[0];
                }
            }

            throw new InvalidOperationException();
        }
    }

    public class CqlQuery<TEntity> : CqlQueryBase<TEntity>, IQueryable, IQueryable<TEntity>, IOrderedQueryable
    {
        internal CqlQuery()
        {
            InternalInitialize(Expression.Constant(this), (Table<TEntity>)this);
        }

        internal CqlQuery(Expression expression, IQueryProvider table) : base(expression,table)
        {
        }

        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new InvalidOperationException("Did you forget to Execute()?");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IQueryProvider Provider
        {
            get { return GetTable() as IQueryProvider; }
        }

        public override string CqlString()
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return eval.Query;
        }

        public IEnumerable<TEntity> Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return InternalBeginExecute(eval.Query, eval.AlternativeMapping, callback, state); 
        }

        public IEnumerable<TEntity> EndExecute(IAsyncResult ar)
        {
            using (var outp = InternalEndExecute(ar))
            {
                QueryTrace = outp.QueryTrace;

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                var rows = outp.GetRows();
                var tag = (CqlQueryTag)Session.GetTag(ar);
                foreach (var row in rows)
                {
                    yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, tag.AlternativeMapping);
                }
            }
        }
    }

    public abstract class CqlCommand : Query
    {
        public abstract string GetCql();
        public void Execute()
        {
            EndExecute(BeginExecute(null, null));
        }

        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }
        
        internal CqlCommand(Expression expression, IQueryProvider table)
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


        public QueryTrace QueryTrace { get; private set; }

        public override CassandraRoutingKey RoutingKey
        {
            get { return null; }
        }

        protected override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return BeginExecute(callback, state);
        }

        protected override CqlRowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return InternalEndExecute(ar);
        }

        protected struct CqlQueryTag
        {
            public Session Session;
        }
        
        protected IAsyncResult InternalBeginExecute(string cqlQuery, AsyncCallback callback, object state)
        {
            var session = GetTable().GetSession();
            return session.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(IsTracing).SetConsistencyLevel(ConsistencyLevel),
                                new CqlQueryTag() {Session = session }, callback, state);
        }
        
        protected CqlRowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var outp = ctx.EndExecute(ar);
            QueryTrace = outp.QueryTrace;
            return outp;
        }

        public virtual IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return InternalBeginExecute(GetCql(), callback, state);
        }

        public virtual void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }
    }

    public class CqlDelete : CqlCommand
    {
        internal CqlDelete(Expression expression, IQueryProvider table) : base(expression, table) { }

        public override string GetCql()
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return eval.DeleteQuery;
        }
    }

    public class CqlInsert<TEntity> : CqlCommand
    {
        private readonly TEntity _entity;

        internal CqlInsert(TEntity entity, IQueryProvider table) : base(null,table)
        {
            this._entity = entity;
        }

        public override string GetCql()
        {
            return CqlQueryTools.GetInsertCQL(_entity, (GetTable()).GetTableName());
        }
    }

    public class CqlUpdate : CqlCommand
    {
        internal CqlUpdate(Expression expression, IQueryProvider table) : base(expression, table) {}

        public override string GetCql()
        {
            var eval = new CqlQueryEvaluator(GetTable());
            eval.Evaluate(Expression);
            return eval.UpdateQuery;
        }
    }
}
