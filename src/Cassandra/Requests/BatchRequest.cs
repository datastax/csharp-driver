//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.IO;
using System.Linq;

namespace Cassandra.Requests
{
    internal class BatchRequest : ICqlRequest
    {
        public const byte OpCode = 0x0D;

        private FrameHeader.HeaderFlag _headerFlags;
        private readonly QueryProtocolOptions.QueryFlags _batchFlags = 0;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly BatchType _type;
        private readonly DateTimeOffset? _timestamp;
        private readonly ConsistencyLevel? _serialConsistency;

        public ConsistencyLevel Consistency { get; set; }

        public IDictionary<string, byte[]> Payload { get; set; }

        public int ProtocolVersion { get; set; }

        public BatchRequest(int protocolVersion, BatchStatement statement, ConsistencyLevel consistency)
        {
            ProtocolVersion = protocolVersion;
            if (ProtocolVersion < 2)
            {
                throw new NotSupportedException("Batch request is supported in C* >= 2.0.x");
            }

            _type = statement.BatchType;
            _requests = statement.Queries
                .Select(q => q.CreateBatchRequest(ProtocolVersion))
                .ToArray();
            Consistency = consistency;
            _timestamp = statement.Timestamp;
            if (statement.IsTracing)
            {
                _headerFlags = FrameHeader.HeaderFlag.Tracing;
            }
            if (statement.SerialConsistencyLevel != ConsistencyLevel.Any)
            {
                if (protocolVersion < 3)
                {
                    throw new NotSupportedException("Serial consistency level for BATCH request is supported in Cassandra 2.1 or above.");
                }
                if (statement.SerialConsistencyLevel < ConsistencyLevel.Serial)
                {
                    throw new RequestInvalidException("Non-serial consistency specified as a serial one.");
                }
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithSerialConsistency;
                _serialConsistency = statement.SerialConsistencyLevel;
            }
            if (_timestamp != null)
            {
                if (protocolVersion < 3)
                {
                    throw new NotSupportedException("Timestamp for BATCH request is supported in Cassandra 2.1 or above.");
                }
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithDefaultTimestamp;
            }
        }

        public int WriteFrame(short streamId, MemoryStream stream)
        {
            //protocol v2: <type><n><query_1>...<query_n><consistency>
            //protocol v3: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var wb = new FrameWriter(stream);
            if (Payload != null)
            {
                _headerFlags |= FrameHeader.HeaderFlag.CustomPayload;
            }
            wb.WriteFrameHeader((byte)ProtocolVersion, (byte)_headerFlags, streamId, OpCode);
            if (Payload != null)
            {
                //A custom payload for this request
                wb.WriteBytesMap(Payload);
            }
            wb.WriteByte((byte) _type);
            wb.WriteInt16((short) _requests.Count);
            foreach (var br in _requests)
            {
                br.WriteToBatch((byte)ProtocolVersion, wb);
            }
            wb.WriteInt16((short) Consistency);
            if (ProtocolVersion >= 3)
            {
                wb.WriteByte((byte)_batchFlags);
            }
            if (_serialConsistency != null)
            {
                wb.WriteInt16((short)_serialConsistency.Value);
            }
            if (_timestamp != null)
            {
                //Expressed in microseconds
                wb.WriteLong(TypeCodec.ToUnixTime(_timestamp.Value).Ticks / 10);
            }
            return wb.Close();
        }
    }
}
