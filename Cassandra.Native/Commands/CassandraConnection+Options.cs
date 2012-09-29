using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteOptions(AsyncCallback callback, object state)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, this, "OPTIONS");

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

        public IOutput EndExecuteOptions(IAsyncResult result)
        {
            return Internal.AsyncResult<IOutput>.End(result, this, "OPTIONS");
        }

        public IOutput ExecuteOptions()
        {
            var r = BeginExecuteOptions(null, null);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteOptions(r);
        }
    }
}
