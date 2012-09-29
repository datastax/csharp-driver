using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra.Native
{
    public partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQuery(string cqlQuery, AsyncCallback callback, object state)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, this, "QUERY");

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

        public IOutput EndExecuteQuery(IAsyncResult result)
        {
            return Internal.AsyncResult<IOutput>.End(result, this, "QUERY");
        }

        public IOutput ExecuteQuery(string cqlQuery)
        {
            var r = BeginExecuteQuery(cqlQuery, null, null);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteQuery(r);
        }
    }
}
