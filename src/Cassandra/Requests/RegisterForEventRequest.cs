//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Collections.Generic;

namespace Cassandra
{
    internal class RegisterForEventRequest : IRequest
    {
        public const byte OpCode = 0x0B;

        private readonly List<string> _eventTypes;
        private readonly int _streamId;

        public RegisterForEventRequest(int streamId, CassandraEventType eventTypes)
        {
            _streamId = streamId;
            _eventTypes = new List<string>();
            if ((eventTypes & CassandraEventType.StatusChange) == CassandraEventType.StatusChange)
                _eventTypes.Add("STATUS_CHANGE");
            if ((eventTypes & CassandraEventType.TopologyChange) == CassandraEventType.TopologyChange)
                _eventTypes.Add("TOPOLOGY_CHANGE");
            if ((eventTypes & CassandraEventType.SchemaChange) == CassandraEventType.SchemaChange)
                _eventTypes.Add("SCHEMA_CHANGE");
        }

        public RequestFrame GetFrame(byte protocolVersionByte)
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(protocolVersionByte, 0x00, (byte) _streamId, OpCode);
            wb.WriteStringList(_eventTypes);
            return wb.GetFrame();
        }
    }
}