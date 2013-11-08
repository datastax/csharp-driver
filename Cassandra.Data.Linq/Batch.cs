//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
﻿using System;
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

        private readonly BatchStatement _batchScript = new BatchStatement();
        private BatchType _batchType = BatchType.Logged;
        protected DateTimeOffset? _timestamp = null;

        public void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
                _batchType = BatchType.Counter;
            _batchScript.AddQuery(cqlCommand);
        }

        public bool IsEmpty { get { return _batchScript.IsEmpty; } }

        public new Batch SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public Batch SetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
            return this;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("BEGIN " + (_batchType == BatchType.Counter ? "COUNTER " : "") + "BATCH");
            foreach (var q in _batchScript.Queries)
                sb.AppendLine(q.ToString() + ";");
            sb.Append("APPLY BATCH");
            return sb.ToString();
        }

        public void Append(IEnumerable<CqlCommand> cqlCommands)
        {
            foreach (var cmd in cqlCommands)
                Append(cmd);
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
            if (_batchScript.IsEmpty)
                throw new ArgumentException("Batch is empty");
         
            return _session.BeginExecute(_batchScript.SetBatchType(_batchType).EnableTracing(IsTracing).SetConsistencyLevel(ConsistencyLevel),
                                    new CqlQueryTag() { Session = _session }, callback, state);
        }

        public void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }

        public override RoutingKey RoutingKey
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

        private RowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag)Session.GetTag(ar);
            var ctx = tag.Session;
            var outp = ctx.EndExecute(ar);
            QueryTrace = outp.Info.QueryTrace;
            return outp;
        }

        protected override RowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            if (!ReferenceEquals(_session, session))
                throw new ArgumentOutOfRangeException("session");
            return InternalEndExecute(ar);
        }

    }
}
