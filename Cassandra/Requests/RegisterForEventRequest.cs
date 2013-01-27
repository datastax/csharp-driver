using System.Collections.Generic;

namespace Cassandra
{
    internal class RegisterForEventRequest : IRequest
    {
        public const byte OpCode = 0x0B;

        readonly int _streamId;
        readonly List<string> _eventTypes;

        public RegisterForEventRequest(int streamId, CassandraEventType eventTypes)
        {
            this._streamId = streamId;
            this._eventTypes = new List<string>();
            if ((eventTypes & CassandraEventType.StatusChange) == CassandraEventType.StatusChange)
                this._eventTypes.Add("STATUS_CHANGE");
            if ((eventTypes & CassandraEventType.TopologyChange) == CassandraEventType.TopologyChange)
                this._eventTypes.Add("TOPOLOGY_CHANGE");
            if ((eventTypes & CassandraEventType.SchemaChange) == CassandraEventType.SchemaChange)
                this._eventTypes.Add("SCHEMA_CHANGE");
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteStringList(_eventTypes);
            return wb.GetFrame();
        }
    }
}
