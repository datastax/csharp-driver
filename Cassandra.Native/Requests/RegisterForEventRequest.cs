using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class RegisterForEventRequest : IRequest
    {
        public const byte OpCode = 0x0B;

        int streamId;
        List<string> eventTypes;

        public RegisterForEventRequest(int streamId, CassandraEventType eventTypes)
        {
            this.streamId = streamId;
            this.eventTypes = new List<string>();
            if ((eventTypes & CassandraEventType.StatusChange) == CassandraEventType.StatusChange)
                this.eventTypes.Add("STATUS_CHANGE");
            if ((eventTypes & CassandraEventType.TopologyChange) == CassandraEventType.TopologyChange)
                this.eventTypes.Add("TOPOLOGY_CHANGE");
            if ((eventTypes & CassandraEventType.SchemaChange) == CassandraEventType.SchemaChange)
                this.eventTypes.Add("SCHEMA_CHANGE");
        }

        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteStringList(eventTypes);
            return wb.GetFrame();
        }
    }
}
