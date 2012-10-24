using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteQueryCredentials(IDictionary<string, string> credentials, AsyncCallback callback, object state, object owner)
        {
            var socketStream = CreateSocketStream();

            AsyncResult<IOutput> ar = new AsyncResult<IOutput>(callback, state, owner, "CREDENTIALS");

            BeginJob(ar, new Action<int>((streamId) =>
            {
                Evaluate(new CredentialsRequest(streamId, credentials), ar, streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ReadyResponse)
                        JobFinished(ar, streamId, new OutputVoid());
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response, streamId = streamId });

                }), socketStream);
            }), socketStream);

            return ar;
        }

        public IOutput EndExecuteQueryCredentials(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "CREDENTIALS");
        }

        public IOutput ExecuteCredentials(IDictionary<string, string> credentials)
        {
            var r = BeginExecuteQueryCredentials(credentials, null, null, this);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteQueryCredentials(r, this);
        }
    }
}
