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
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Cassandra.RequestHandlers
{
    internal class BatchRequestHandler : RequestHandler
    {
        public BatchType BatchType;
        public List<Statement> Queries;
        public bool IsTracing;
        public Stopwatch StartedAt;

        override public void Connect(Session owner, bool moveNext, out int streamId)
        {
            StartedAt = Stopwatch.StartNew();
            base.Connect(owner, moveNext, out streamId);
        }

        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginBatch(streamId, BatchType, Queries, owner.RequestCallback, this, owner, Consistency ?? owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), IsTracing);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = ProcessResponse(Connection.EndBatch(ar, owner), owner);
        }

        override public void Complete(Session owner, object value, Exception exc = null)
        {
            try
            {
                var ar = LongActionAc as AsyncResult<RowSet>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    var rowset = value as RowSet;
                    if (rowset == null)
                    {
                        rowset = new RowSet();
                    }
                    rowset.Info.SetTriedHosts(TriedHosts);
                    rowset.Info.SetAchievedConsistency(Consistency ?? owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
                    ar.SetResult(rowset);
                    ar.Complete();
                }
            }
            finally
            {
                var ts = StartedAt.ElapsedTicks;
                CassandraCounters.IncrementCqlQueryCount();
                CassandraCounters.IncrementCqlQueryBeats((ts * 1000000000));
                CassandraCounters.UpdateQueryTimeRollingAvrg((ts * 1000000000) / Stopwatch.Frequency);
                CassandraCounters.IncrementCqlQueryBeatsBase();
            }
        }
    }
}
