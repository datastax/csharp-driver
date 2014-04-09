using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

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

        protected IAsyncResult InternalBeginExecute(string cqlQuery, object[] values, Dictionary<string, Tuple<string, object, int>> mappingNames,
                                                    Dictionary<string, string> alter, AsyncCallback callback, object state)
        {
            var session = GetTable().GetSession();
            SimpleStatement stmt = new SimpleStatement(cqlQuery).BindObjects(values);
            this.CopyQueryPropertiesTo(stmt);
            return session.BeginExecute(stmt,
                                        new CqlQueryTag {Mappings = mappingNames, Alter = alter, Session = session}, callback, state);
        }

        protected RowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            RowSet outp = ctx.EndExecute(ar);
            QueryTrace = outp.Info.QueryTrace;
            return outp;
        }

        public abstract IAsyncResult BeginExecute(AsyncCallback callback, object state);

        protected struct CqlQueryTag
        {
            public Dictionary<string, string> Alter;
            public Dictionary<string, Tuple<string, object, int>> Mappings;
            public ISession Session;
        }
    }
}