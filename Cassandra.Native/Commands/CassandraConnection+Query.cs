using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, object owner, ConsistencyLevel consistency)
        {
            return BeginJob(callback, state, owner, "QUERY", new Action<int>((streamId) =>
            {
                Evaluate(new QueryRequest(streamId, cqlQuery, consistency), streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(streamId, (response as ResultResponse).Output);
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() { Response = response, streamId = streamId });

                }));
            }));
        }

        public IOutput EndQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "QUERY");
        }

        public IOutput Query(string cqlQuery, ConsistencyLevel consistency)
        {
            var r = BeginQuery(cqlQuery, null, null, this, consistency);
            return EndQuery(r, this);
        }
    }
}
