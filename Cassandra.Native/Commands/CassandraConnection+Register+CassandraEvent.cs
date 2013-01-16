using System;
using System.Net;

namespace Cassandra
{
    public class CassandraEventArgs : EventArgs
    {
    }

    public class TopopogyChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            NewNode,
            RemovedNode
        };

        public Reason What;
        public IPAddress Address;
    }

    public class StatusChangeEventArgs: CassandraEventArgs
    {
        public enum Reason
        {
            Up,
            Down
        };
        public Reason What;
        public IPAddress Address;
    }

    public class SchemaChangeEventArgs:CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };
        public Reason What;
        public string Keyspace;
        public string Table;
    }

    public delegate void CassandraEventHandler(object sender, CassandraEventArgs e);

    [Flags]
    public enum CassandraEventType { TopologyChange = 0x01, StatusChange = 0x02, SchemaChange = 0x03 }

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
            throw new DriverInternalError("Unexpected response frame");
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
                        _protocolErrorHandlerAction(new ErrorActionParam() {Response = response, StreamId = streamId });

                }));
            }));
        }

        public IOutput EndRegisterForCassandraEvent(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "REGISTER");
        }

        public IOutput RegisterForCassandraEvent(CassandraEventType eventTypes)
        {
            return EndRegisterForCassandraEvent(BeginRegisterForCassandraEvent(eventTypes, null, null, this), this);
        }
    }
}
