//
//      Copyright (C) DataStax Inc.
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

namespace Cassandra.Requests
{
    internal class RegisterForEventRequest : BaseRequest
    {
        public const byte RegisterOpCode = 0x0B;

        private readonly List<string> _eventTypes;

        public RegisterForEventRequest(CassandraEventType eventTypes) : base(false, null)
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

        protected override byte OpCode => RegisterForEventRequest.RegisterOpCode;

        /// <inheritdoc />
        public override ResultMetadata ResultMetadata => null;

        protected override void WriteBody(FrameWriter wb)
        {
            wb.WriteStringList(_eventTypes);
        }
    }
}