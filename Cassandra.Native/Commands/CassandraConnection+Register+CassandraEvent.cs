using System;
using System.Collections.Generic;
using System.Text;
using System.Net;

namespace Cassandra.Native
{
    public class CassandraEventArgs : EventArgs
    {
        public CassandraEventType CassandraEventType;
        public IPEndPoint IPEndPoint;
        public string Message;
    }

    public delegate void CassandraEventHandler(object sender, CassandraEventArgs e);

    [Flags]
    public enum CassandraEventType { TopologyChange = 0x01, StatusChange = 0x02 }

    internal partial class CassandraConnection : IDisposable
    {

        public event CassandraEventHandler CassandraEvent;

        private void EventOccured(ResponseFrame frame)
        {
            var response = FrameParser.Parse(frame);
            if (response is EventResponse)
            {
                if (CassandraEvent != null)
                    CassandraEvent.Invoke(this, (response as EventResponse).CassandraEventArgs);
                return;
            }
            throw new CassandraClientProtocolViolationException("Unexpected response frame");
        }

        public IAsyncResult BeginRegisterForCassandraEvent(CassandraEventType eventTypes, AsyncCallback callback, object state, object owner)
        {
            return BeginJob(callback, state, owner, "REGISTER", new Action<int>((streamId) =>
            {
                Evaluate(new RegisterForEventRequest(streamId, eventTypes),streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ReadyResponse)
                        JobFinished( streamId, new OutputVoid());
                    else
                        ProtocolErrorHandlerAction(new ErrorActionParam() {Response = response, streamId = streamId });

                }));
            }));
        }

        public IOutput EndRegisterForCassandraEvent(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "REGISTER");
        }

        public IOutput RegisterForCassandraEvent(CassandraEventType eventTypes)
        {
            var r = BeginRegisterForCassandraEvent(eventTypes, null, null, this);
            return EndRegisterForCassandraEvent(r, this);
        }
    }
}
