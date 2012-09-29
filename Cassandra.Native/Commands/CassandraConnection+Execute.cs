using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteExecute(int Id, Metadata Metadata, object[] values, AsyncCallback callback, object state)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, this, "EXECUTE");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new ExecuteRequest(streamId, Id, Metadata, values), ar, streamId, new Action<ResponseFrame>((frame2) =>
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

        public IOutput EndExecuteExecute(IAsyncResult result)
        {
            return Internal.AsyncResult<IOutput>.End(result, this, "EXECUTE");
        }

        public IOutput ExecuteExecute(int Id, Metadata Metadata, object[] values)
        {
            var r = BeginExecuteExecute(Id, Metadata, values, null, null);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteExecute(r);
        }
    }
}
