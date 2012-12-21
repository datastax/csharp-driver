using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQuery(byte[] Id, Metadata Metadata, object[] values, AsyncCallback callback, object state, object owner, ConsistencyLevel consistency)
        {
            return BeginJob(callback, state, owner, "EXECUTE", new Action<int>((streamId) =>
            {
                Evaluate(new ExecuteRequest(streamId, Id, Metadata, values, consistency), streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(streamId, (response as ResultResponse).Output);
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() {  Response = response, streamId = streamId });

                }));
            }));
        }

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(byte[] Id, Metadata Metadata, object[] values, ConsistencyLevel consistency)
        {
            var r = BeginExecuteQuery(Id, Metadata, values, null, null, this, consistency);
            return EndExecuteQuery(r, this);
        }
    }
}
