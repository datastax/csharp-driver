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

        private readonly ConsistencyLevel _consistency;
        private readonly byte _flags;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly int _streamId;
        private readonly BatchType _type;

        public BatchRequest(int streamId, BatchType type, ICollection<IQueryRequest> requests, ConsistencyLevel consistency, bool tracingEnabled)
        {
            _type = type;
            _requests = requests;
            _streamId = streamId;
            _consistency = consistency;
            if (tracingEnabled)
                _flags = 0x02;
        }

        public RequestFrame GetFrame(byte protocolVersionByte)
        {
            if (protocolVersionByte != RequestFrame.ProtocolV2RequestVersionByte)
                throw new NotSupportedException("Batch request is supported in C* >= 2.0.x");
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(protocolVersionByte, _flags, (byte) _streamId, OpCode);
            wb.WriteByte((byte) _type);
            wb.WriteInt16((short) _requests.Count);
            foreach (IQueryRequest br in _requests)
                br.WriteToBatch(wb);
            wb.WriteInt16((short) _consistency);
            return wb.GetFrame();
        }
    }
}