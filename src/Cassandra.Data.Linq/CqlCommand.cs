using System;
using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public abstract class CqlCommand : SimpleStatement
    {
        protected abstract string GetCql(out object[] values);
        public void Execute()
        {
            EndExecute(BeginExecute(null, null));
        }

        public override string QueryString
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryString;
            }
        }

        public override object[] QueryValues
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryValues;
            }
        }

        protected int? _ttl = null;
        protected DateTimeOffset? _timestamp = null;
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }

        public new CqlCommand SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlCommand SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        public CqlCommand SetTTL(int seconds)
        {
            _ttl = seconds;
            return this;
        }

        public CqlCommand SetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
            return this;
        }

        internal CqlCommand(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;

        }

        protected void InitializeStatement()
        {
            object[] values;
            var query = GetCql(out values);
            SetQueryString(query);
            BindObjects(values);
        }

        public ITable GetTable()
        {
            return (_table as ITable);
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        public QueryTrace QueryTrace { get; private set; }

        protected  override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return InternalBeginExecute(callback, state);
        }

        protected  override RowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            if (!ReferenceEquals(GetTable().GetSession(), session))
                throw new ArgumentOutOfRangeException("session");
            return InternalEndExecute(ar);
        }

        protected struct CqlQueryTag
        {
            public Session Session;
        }

        protected IAsyncResult InternalBeginExecute(AsyncCallback callback, object state)
        {
            InitializeStatement();
            var session = GetTable().GetSession();
            return base.BeginSessionExecute(session, new CqlQueryTag() { Session = session }, callback, state);
        }

        protected RowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var outp = base.EndSessionExecute(ctx, ar);
            QueryTrace = outp.Info.QueryTrace;
            return outp;
        }

        public virtual IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return InternalBeginExecute(callback, state);
        }

        public virtual void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }
    }
}