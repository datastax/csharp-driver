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

using System;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    public abstract class Batch : Statement
    {
        protected readonly ISession _session;

        protected BatchType _batchType = BatchType.Logged;
        protected DateTimeOffset? _timestamp = null;

        public abstract bool IsEmpty { get; }

        public override RoutingKey RoutingKey
        {
            get { return null; }
        }

        public QueryTrace QueryTrace { get; private set; }

        internal Batch(ISession session)
        {
            _session = session;
        }

        public abstract void Append(CqlCommand cqlCommand);

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

        public void Append(IEnumerable<CqlCommand> cqlCommands)
        {
            foreach (CqlCommand cmd in cqlCommands)
                Append(cmd);
        }

        public void Execute()
        {
            EndExecute(BeginExecute(null, null));
        }

        public abstract IAsyncResult BeginExecute(AsyncCallback callback, object state);

        public void EndExecute(IAsyncResult ar)
        {
            InternalEndExecute(ar);
        }

        private RowSet InternalEndExecute(IAsyncResult ar)
        {
            var tag = (CqlQueryTag) Session.GetTag(ar);
            var ctx = tag.Session;
            RowSet outp = ctx.EndExecute(ar);
            QueryTrace = outp.Info.QueryTrace;
            return outp;
        }

        protected struct CqlQueryTag
        {
            public ISession Session;
        }
    }
}