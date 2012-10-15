using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQueryOptions(AsyncCallback callback, object state, object owner)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, owner, "OPTIONS");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new OptionsRequest(streamId), ar, streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is SupportedResponse)
                        JobFinished(ar, streamId, (response as SupportedResponse).Output);
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response, streamId = streamId });

                }), socketStream);
            }), socketStream, true);

            return ar;
        }

        public IOutput EndExecuteQueryOptions(IAsyncResult result, object owner)
        {
            return Internal.AsyncResult<IOutput>.End(result, owner, "OPTIONS");
        }

        public IOutput ExecuteOptions()
        {
            var r = BeginExecuteQueryOptions(null, null, this);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteQueryOptions(r, this);
        }
    }
}
