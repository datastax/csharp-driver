using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, object owner)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, owner, "QUERY");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new QueryRequest(streamId, cqlQuery), ar, streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(ar, streamId, (response as ResultResponse).Output);
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response, streamId = streamId });

                }), socketStream);
            }), socketStream);

            return ar;
        }

        public IOutput EndQuery(IAsyncResult result, object owner)
        {
            return Internal.AsyncResult<IOutput>.End(result, owner, "QUERY");
        }

        public IOutput Query(string cqlQuery)
        {
            var r = BeginQuery(cqlQuery, null, null, this);
            r.AsyncWaitHandle.WaitOne();
            return EndQuery(r, this);
        }
    }
}
