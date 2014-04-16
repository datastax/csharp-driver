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
            Connection.BeginBatch(streamId, BatchType, Queries, owner.ClbNoQuery, this, owner, Consistency ?? owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), IsTracing);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = ProcessRowset(Connection.EndBatch(ar, owner), owner);
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
