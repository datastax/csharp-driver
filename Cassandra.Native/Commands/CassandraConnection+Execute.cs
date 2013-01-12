using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, AsyncCallback callback, object state, object owner, ConsistencyLevel consistency)
        {
            return BeginJob(callback, state, owner, "EXECUTE", new Action<int>((streamId) =>
            {
                Evaluate(new ExecuteRequest(streamId, Id, Metadata, values, consistency), streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(streamId, (response as ResultResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() {  Response = response, StreamId = streamId });

                }));
            }));
        }

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, ConsistencyLevel consistency)
        {
            return EndExecuteQuery(BeginExecuteQuery(Id, Metadata, values, null, null, this, consistency), this);
        }
    }
}
