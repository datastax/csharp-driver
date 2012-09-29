using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public partial class CassandraConnection : IDisposable
    {
        public IAsyncResult BeginExecuteCredentials(IDictionary<string, string> credentials, AsyncCallback callback, object state)
        {
            var socketStream = CreateSocketStream();

            Internal.AsyncResult<IOutput> ar = new Internal.AsyncResult<IOutput>(callback, state, this, "CREDENTIALS");

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

        public IOutput EndExecuteCredentials(IAsyncResult result)
        {
            return Internal.AsyncResult<IOutput>.End(result, this, "CREDENTIALS");
        }

        public IOutput ExecuteCredentials(IDictionary<string, string> credentials)
        {
            var r = BeginExecuteCredentials(credentials, null, null);
            r.AsyncWaitHandle.WaitOne();
            return EndExecuteCredentials(r);
        }
    }
}
