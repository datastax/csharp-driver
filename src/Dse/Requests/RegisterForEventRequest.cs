//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class RegisterForEventRequest : IRequest
    {
        public const byte OpCode = 0x0B;
        private readonly List<string> _eventTypes;

        public RegisterForEventRequest(CassandraEventType eventTypes)
        {
            _eventTypes = new List<string>();
            if ((eventTypes & CassandraEventType.StatusChange) == CassandraEventType.StatusChange)
            {
                _eventTypes.Add("STATUS_CHANGE");
            }
            if ((eventTypes & CassandraEventType.TopologyChange) == CassandraEventType.TopologyChange)
            {
                _eventTypes.Add("TOPOLOGY_CHANGE");
            }
            if ((eventTypes & CassandraEventType.SchemaChange) == CassandraEventType.SchemaChange)
            {
                _eventTypes.Add("SCHEMA_CHANGE");
            }
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            wb.WriteStringList(_eventTypes);
            return wb.Close();
        }
    }
}
