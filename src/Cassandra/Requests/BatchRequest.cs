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
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class BatchRequest : ICqlRequest
    {
        private const byte OpCode = 0x0D;

        private FrameHeader.HeaderFlag _headerFlags;
        private readonly QueryProtocolOptions.QueryFlags _batchFlags = 0;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly BatchType _type;
        private readonly long? _timestamp;
        private readonly ConsistencyLevel _serialConsistency;

        public ConsistencyLevel Consistency { get; set; }

        public IDictionary<string, byte[]> Payload { get; set; }

        public BatchRequest(ProtocolVersion protocolVersion, BatchStatement statement, ConsistencyLevel consistency, Configuration config)
        {
            if (!protocolVersion.SupportsBatch())
            {
                throw new NotSupportedException("Batch request is supported in C* >= 2.0.x");
            }
            _type = statement.BatchType;
            _requests = statement.Queries
                .Select(q => q.CreateBatchRequest(protocolVersion))
                .ToArray();
            Consistency = consistency;
            if (statement.IsTracing)
            {
                _headerFlags = FrameHeader.HeaderFlag.Tracing;
            }

            _serialConsistency = config.QueryOptions.GetSerialConsistencyLevelOrDefault(statement);
            _batchFlags |= QueryProtocolOptions.QueryFlags.WithSerialConsistency;

            _timestamp = GetRequestTimestamp(protocolVersion, statement, config.Policies);
            if (_timestamp != null)
            {
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithDefaultTimestamp;   
            }
        }

        /// <summary>
        /// Gets the timestamp of the request or null if not defined.
        /// </summary>
        /// <exception cref="NotSupportedException" />
        private static long? GetRequestTimestamp(ProtocolVersion protocolVersion, BatchStatement statement,
                                                 Policies policies)
        {
            if (!protocolVersion.SupportsTimestamp())
            {
                if (statement.Timestamp != null)
                {
                    throw new NotSupportedException(
                        "Timestamp for BATCH request is supported in Cassandra 2.1 or above.");
                }
                return null;
            }
            if (statement.Timestamp != null)
            {
                return TypeSerializer.SinceUnixEpoch(statement.Timestamp.Value).Ticks / 10;
            }
            var timestamp = policies.TimestampGenerator.Next();
            return timestamp != long.MinValue ? (long?) timestamp : null;
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            //protocol v2: <type><n><query_1>...<query_n><consistency>
            //protocol v3: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var protocolVersion = serializer.ProtocolVersion;
            var wb = new FrameWriter(stream, serializer);
            if (Payload != null)
            {
                _headerFlags |= FrameHeader.HeaderFlag.CustomPayload;
            }
            wb.WriteFrameHeader((byte)_headerFlags, streamId, OpCode);
            if (Payload != null)
            {
                //A custom payload for this request
                wb.WriteBytesMap(Payload);
            }
            wb.WriteByte((byte) _type);
            wb.WriteUInt16((ushort) _requests.Count);
            foreach (var br in _requests)
            {
                br.WriteToBatch(wb);
            }
            wb.WriteUInt16((ushort) Consistency);
            if (protocolVersion.SupportsTimestamp())
            {
                wb.WriteByte((byte) _batchFlags);
                wb.WriteUInt16((ushort) _serialConsistency);

                if (_timestamp != null)
                {
                    wb.WriteLong(_timestamp.Value);
                }
            }
            return wb.Close();
        }
    }
}
