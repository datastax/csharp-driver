using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, this, "PREPARE");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new PrepareRequest(streamId, cqlQuery), ar, streamId, new Action<ResponseFrame>((frame2) =>
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

        public IOutput EndPrepareQuery(IAsyncResult result)
        {
            return Internal.AsyncResult<IOutput>.End(result, this, "PREPARE");
        }

        public IOutput PrepareQuery(string cqlQuery)
        {
            var r = BeginPrepareQuery(cqlQuery, null, null);
            r.AsyncWaitHandle.WaitOne();
            return EndPrepareQuery(r);
        }
    }
}
