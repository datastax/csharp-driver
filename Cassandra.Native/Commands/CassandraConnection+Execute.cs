using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQuery(byte[] Id, Metadata Metadata, object[] values, AsyncCallback callback, object state, object owner, CqlConsistencyLevel consistency)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, owner, "EXECUTE");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new ExecuteRequest(streamId, Id, Metadata, values, consistency), ar, streamId, new Action<ResponseFrame>((frame2) =>
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

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return Internal.AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(byte[] Id, Metadata Metadata, object[] values, CqlConsistencyLevel consistency)
        {
            var r = BeginExecuteQuery(Id, Metadata, values, null, null, this, consistency);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteQuery(r, this);
        }
    }
}
