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

        private readonly StringBuilder _batchScript = new StringBuilder();
        private string _batchType = "";
        protected DateTimeOffset? _timestamp = null;

        public void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
                _batchType = "COUNTER ";
            _batchScript.Append(cqlCommand.ToString());
            _batchScript.AppendLine(";");
        }

        public bool IsEmpty { get { return _batchScript.Length == 0; } }

        public new Batch SetConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public Batch SetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
            return this;
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
            if (_batchScript.Length != 0)
            {
                var ctx = _session;
                var cqlQuery = GetCql();
                var stmt = new SimpleStatement(cqlQuery);
                this.CopyQueryPropertiesTo(stmt);
                return ctx.BeginExecute(stmt,
                                    new CqlQueryTag() { Session = ctx }, callback, state);
            }
            throw new ArgumentOutOfRangeException();
        }

        private string GetCql()
        {
            return "BEGIN " + _batchType + "BATCH\r\n" +
                ((_timestamp == null) ? "" : ("USING TIMESTAMP " + (_timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10).ToString() + " ") +
                _batchScript.ToString() + "APPLY " + _batchType + "BATCH";
        }

        public override string ToString()
        {
            return GetCql();
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
