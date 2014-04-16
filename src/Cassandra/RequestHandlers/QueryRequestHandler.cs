using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Cassandra.RequestHandlers
{
    /// <summary>
    /// Sends a query request and parses the response.
    /// </summary>
    internal class QueryRequestHandler : RequestHandler
    {
        public string CqlQuery;
        public bool IsTracing;
        public Stopwatch StartedAt;
        public QueryProtocolOptions QueryPrtclOptions;

        override public void Connect(Session owner, bool moveNext, out int streamId)
        {
            StartedAt = Stopwatch.StartNew();
            base.Connect(owner, moveNext, out streamId);
        }

        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginQuery(streamId, CqlQuery, owner.ClbNoQuery, this, owner, IsTracing, QueryProtocolOptions.CreateFromQuery(Query, owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel()), Consistency);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = owner.ProcessRowset(Connection.EndQuery(ar, owner));
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
                    rowset.Info.SetAchievedConsistency(Consistency ?? QueryPrtclOptions.Consistency);
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
