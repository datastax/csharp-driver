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
    internal class QueryRequestHandler : ExecuteQueryRequestHandler
    {
        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginQuery(streamId, CqlQuery, owner.ClbNoQuery, this, owner, IsTracing, QueryProtocolOptions.CreateFromQuery(Query, owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel()), Consistency);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = ProcessRowset(Connection.EndQuery(ar, owner), owner);
        }
    }
}
