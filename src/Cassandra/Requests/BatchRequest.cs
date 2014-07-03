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
using System;
using System.Collections.Generic;

namespace Cassandra
{
    internal class BatchRequest : IRequest
    {
        public const byte OpCode = 0x0D;

        public ConsistencyLevel Consistency { get; set; }
        public int ProtocolVersion { get; set; }

        private readonly byte _flags;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly BatchType _type;

        public BatchRequest(int protocolVersion, BatchType type, ICollection<IQueryRequest> requests, ConsistencyLevel consistency, bool tracingEnabled)
        {
            ProtocolVersion = protocolVersion;
            if (ProtocolVersion < 2)
            {
                throw new NotSupportedException("Batch request is supported in C* >= 2.0.x");
            }
            _type = type;
            _requests = requests;
            Consistency = consistency;
            if (tracingEnabled)
            {
                _flags = 0x02;
            }
        }

        public RequestFrame GetFrame(short streamId)
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader((byte)ProtocolVersion, _flags, streamId, OpCode);
            wb.WriteByte((byte) _type);
            wb.WriteInt16((short) _requests.Count);
            foreach (IQueryRequest br in _requests)
            {
                br.WriteToBatch((byte)ProtocolVersion, wb);
            }
            wb.WriteInt16((short) Consistency);
            return wb.GetFrame();
        }
    }
}