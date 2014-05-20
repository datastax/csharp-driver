using System;
using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public abstract class CqlCommand : SimpleStatement
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;
        protected DateTimeOffset? _timestamp = null;
        protected int? _ttl = null;

        /// <inheritdoc />
        public override string QueryString
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryString;
            }
        }

        /// <inheritdoc />
        public override object[] QueryValues
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryValues;
            }
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        public QueryTrace QueryTrace { get; private set; }

        internal CqlCommand(Expression expression, IQueryProvider table)
        {
            _expression = expression;
            _table = table;
        }

        protected abstract string GetCql(out object[] values);

        public void Execute()
        {
            EndExecute(BeginExecute(null, null));
        }

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

        protected void InitializeStatement()
        {
            object[] values;
            string query = GetCql(out values);
            SetQueryString(query);
            BindObjects(values);
        }

        public ITable GetTable()
        {
            return (_table as ITable);
        }

        protected IAsyncResult InternalBeginExecute(AsyncCallback callback, object state)
        {
            InitializeStatement();
            var session = GetTable().GetSession();
            return session.BeginExecute(this, new CqlQueryTag { Session = session }, callback, state);
        }

        protected RowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            RowSet rowSet = ctx.EndExecute(ar);
            QueryTrace = rowSet.Info.QueryTrace;
            return rowSet;
        }

        /// <summary>
        /// Starts executing the statement async
        /// </summary>
        public virtual IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return InternalBeginExecute(callback, state);
        }

        /// <summary>
        /// Starts the async executing of the statement
        /// </summary>
        public virtual void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }

        protected struct CqlQueryTag
        {
            public ISession Session;
        }
    }
}