using System;

namespace Cassandra
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQueryOptions(AsyncCallback callback, object state, object owner)
        {
            return BeginJob(callback, state, owner, "OPTIONS", new Action<int>((streamId) =>
            {
                Evaluate(new OptionsRequest(streamId), streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is SupportedResponse)
                        JobFinished( streamId, (response as SupportedResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, StreamId = streamId });

                }));
            }), true);
        }

        public IOutput EndExecuteQueryOptions(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "OPTIONS");
        }

        public IOutput ExecuteOptions()
        {
            return EndExecuteQueryOptions(BeginExecuteQueryOptions(null, null, this), this);
        }
    }
}
