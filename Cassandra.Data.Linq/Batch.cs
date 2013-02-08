using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    public class Batch
    {
        readonly Session _session;
        internal Batch(Session session)
        {
            _session = session;
        }

        private List<ICqlCommand> _additionalCommands = new List<ICqlCommand>();

        public void AppendCommand(ICqlCommand cqlCommand)
        {
            _additionalCommands.Add(cqlCommand);
        }

        public void Execute(ConsistencyLevel consistencyLevel)
        {
            EndExecute(BeginExecute(consistencyLevel, null, null));
        }

        private struct CqlQueryTag
        {
            public Session Session;
            public List<ICqlCommand> TraceableCommands;
        }

        public IAsyncResult BeginExecute(ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            bool enableTracing = false;
            StringBuilder batchScript = new StringBuilder();
            List<ICqlCommand> traceableCommands = null;
            string BT = "";
            foreach (var additional in _additionalCommands)
            {
                if(additional.IsQueryTraceEnabled())
                {
                    enableTracing = true;
                    if (traceableCommands == null)
                        traceableCommands = new List<ICqlCommand>();
                    traceableCommands.Add(additional);
                }
                if (additional.GetTable().GetTableType() == TableType.Counter)
                    BT = "COUNTER ";
                batchScript.AppendLine(additional.GetCql() + ";");
            }
            if (batchScript.Length != 0)
            {
                var ctx = _session;
                var cqlQuery = "BEGIN " + BT + "BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY " + BT + "BATCH";
                return ctx.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(enableTracing).SetConsistencyLevel(consistencyLevel),
                                    new CqlQueryTag() { Session = ctx, TraceableCommands = traceableCommands }, callback, state);
            }
            throw new ArgumentOutOfRangeException();
        }

        public void EndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var res = ctx.EndExecute(ar);
            if (tag.TraceableCommands != null)
                foreach (var command in tag.TraceableCommands)
                    command.SetQueryTrace(res.QueryTrace);
        }

    }
}
