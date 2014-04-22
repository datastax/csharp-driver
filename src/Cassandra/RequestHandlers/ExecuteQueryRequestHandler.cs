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
    /// <summary>
    /// Sends an execute request and parses the response.
    /// </summary>
    internal class ExecuteQueryRequestHandler : RequestHandler
    {
        private Stopwatch _startedAt;
        /// <summary>
        /// Prepared query id
        /// </summary>
        public byte[] Id { get; set; }
        /// <summary>
        /// Gets or sets the cql query to be executed
        /// </summary>
        public string CqlQuery { get; set; }
        public RowSetMetadata Metadata { get; set; }
        public QueryProtocolOptions QueryProtocolOptions { get; set; }
        /// <summary>
        /// Determines if the request is being traced in Cassandra
        /// </summary>
        public bool IsTracing { get; set; }

        override public void Connect(Session owner, bool moveNext, out int streamId)
        {
            _startedAt = Stopwatch.StartNew();
            base.Connect(owner, moveNext, out streamId);
        }

        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginExecuteQuery(streamId, Id, CqlQuery, Metadata, owner.RequestCallback, this, owner, IsTracing, QueryProtocolOptions.CreateFromQuery(Statement, owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel()), Consistency);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = ProcessResponse(Connection.EndExecuteQuery(ar, owner), owner);
        }

        override public void Complete(Session owner, object value, Exception exc = null)
        {
            try
            {
                var ar = LongActionAc as AsyncResult<RowSet>;
                if (exc != null)
                {
                    ar.Complete(exc);
                }
                else
                {
                    RowSet rowset = value as RowSet;
                    if (rowset == null)
                    {
                        rowset = new RowSet();
                    }
                    rowset.Info.SetTriedHosts(TriedHosts);
                    rowset.Info.SetAchievedConsistency(Consistency ?? QueryProtocolOptions.Consistency);
                    ar.SetResult(rowset);
                    ar.Complete();
                }
            }
            finally
            {
                var ts = _startedAt.ElapsedTicks;
                CassandraCounters.IncrementCqlQueryCount();
                CassandraCounters.IncrementCqlQueryBeats((ts * 1000000000));
                CassandraCounters.UpdateQueryTimeRollingAvrg((ts * 1000000000) / Stopwatch.Frequency);
                CassandraCounters.IncrementCqlQueryBeatsBase();
            }
        }
    }
}
