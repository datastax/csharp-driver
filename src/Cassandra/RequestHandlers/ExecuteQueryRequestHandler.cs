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
        public byte[] Id;
        public string Cql;
        public RowSetMetadata Metadata;
        public RowSetMetadata ResultMetadata;
        public QueryProtocolOptions QueryProtocolOptions;
        public bool IsTracinig;
        public Stopwatch StartedAt;

        override public void Connect(Session owner, bool moveNext, out int streamId)
        {
            StartedAt = Stopwatch.StartNew();
            base.Connect(owner, moveNext, out streamId);
        }

        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginExecuteQuery(streamId, Id, Cql, Metadata, owner.ClbNoQuery, this, owner, IsTracinig, QueryProtocolOptions.CreateFromQuery(Query, owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel()), Consistency);
        }
        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = owner.ProcessRowset(Connection.EndExecuteQuery(ar, owner), ResultMetadata);
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
                    RowSet rowset = value as RowSet;
                    if (rowset == null)
                        rowset = new RowSet(null, owner, false);
                    rowset.Info.SetTriedHosts(TriedHosts);
                    rowset.Info.SetAchievedConsistency(Consistency ?? QueryProtocolOptions.Consistency);
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
