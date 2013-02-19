using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    public class Batch : Query
    {
        readonly Session _session;

        internal Batch(Session session)
        {
            _session = session;
        }

        private readonly List<CqlCommand> _commands = new List<CqlCommand>();

        public void Append(CqlCommand cqlCommand)
        {
            _commands.Add(cqlCommand);
        }

        public void Append(IEnumerable<CqlCommand> cqlCommands)
        {
            foreach(var cmd in cqlCommands)
                _commands.Add(cmd);
        }

        public void Execute()
        {
            EndExecute(BeginExecute(null, null));
        }

        private struct CqlQueryTag
        {
            public Session Session;
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            StringBuilder batchScript = new StringBuilder();
            string BT = "";
            foreach (var cmd in _commands)
            {
                if (cmd.GetTable().GetTableType() == TableType.Counter)
                    BT = "COUNTER ";
                batchScript.AppendLine(cmd.GetCql());
            }
            if (batchScript.Length != 0)
            {
                var ctx = _session;
                var cqlQuery = "BEGIN " + BT + "BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY " + BT + "BATCH";
                return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(IsTracing).SetConsistencyLevel(ConsistencyLevel),
                                    new CqlQueryTag() { Session = ctx }, callback, state);
            }
            throw new ArgumentOutOfRangeException();
        }

        public void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }

        public override CassandraRoutingKey RoutingKey
        {
            get { return null; }
        }

        protected override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            if (!ReferenceEquals(_session, session))
                throw new ArgumentOutOfRangeException("session");
            return BeginExecute(callback, state);
        }

        public QueryTrace QueryTrace { get; private set; }
        
        private CqlRowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var outp = ctx.EndExecute(ar);
            QueryTrace = outp.QueryTrace;
            return outp;
        }
        
        protected override CqlRowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            if (!ReferenceEquals(_session, session))
                throw new ArgumentOutOfRangeException("session");
            return InternalEndExecute(ar);
        }

    }
}
