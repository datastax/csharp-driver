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

using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.ExecutionProfiles;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class BatchRequest : BaseRequest, ICqlRequest
    {
        private const byte BatchOpCode = 0x0D;

        private readonly QueryProtocolOptions.QueryFlags _batchFlags = 0;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly BatchType _type;
        private readonly long? _timestamp;
        private readonly string _keyspace;

        internal ConsistencyLevel SerialConsistency { get; }

        public ConsistencyLevel Consistency { get; set; }

        protected override byte OpCode => BatchRequest.BatchOpCode;
        
        /// <inheritdoc />
        public override ResultMetadata ResultMetadata => null;

        public BatchRequest(
            ISerializer serializer,
            IDictionary<string, byte[]> payload,
            BatchStatement statement,
            ConsistencyLevel consistency,
            IRequestOptions requestOptions) : base(serializer, statement.IsTracing, payload)
        {
            if (!serializer.ProtocolVersion.SupportsBatch())
            {
                throw new NotSupportedException("Batch request is supported in C* >= 2.0.x");
            }

            if (statement.Timestamp != null && !serializer.ProtocolVersion.SupportsTimestamp())
            {
                throw new NotSupportedException(
                    "Timestamp for BATCH request is supported in Cassandra 2.1 or above.");
            }

            _type = statement.BatchType;
            _requests = statement.Queries
                .Select(q => q.CreateBatchRequest(serializer))
                .ToArray();
            Consistency = consistency;

            if (!serializer.ProtocolVersion.SupportsBatchFlags())
            {
                // if flags are not supported, then the following additional parameters aren't either
                return;
            }
            
            SerialConsistency = requestOptions.GetSerialConsistencyLevelOrDefault(statement);
            _batchFlags |= QueryProtocolOptions.QueryFlags.WithSerialConsistency;

            if (serializer.ProtocolVersion.SupportsTimestamp())
            {
                _timestamp = BatchRequest.GetRequestTimestamp(statement, requestOptions.TimestampGenerator);
            }

            if (_timestamp != null)
            {
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithDefaultTimestamp;
            }

            _keyspace = statement.Keyspace;
            if (serializer.ProtocolVersion.SupportsKeyspaceInRequest() && _keyspace != null)
            {
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithKeyspace;
            }
        }

        /// <summary>
        /// Gets the timestamp of the request or null if not defined.
        /// </summary>
        private static long? GetRequestTimestamp(BatchStatement statement, ITimestampGenerator timestampGenerator)
        {
            if (statement.Timestamp != null)
            {
                return TypeSerializer.SinceUnixEpoch(statement.Timestamp.Value).Ticks / 10;
            }

            var timestamp = timestampGenerator.Next();
            return timestamp != long.MinValue ? (long?)timestamp : null;
        }

        protected override void WriteBody(FrameWriter wb)
        {
            var protocolVersion = wb.Serializer.ProtocolVersion;

            wb.WriteByte((byte)_type);
            wb.WriteUInt16((ushort)_requests.Count);

            foreach (var br in _requests)
            {
                br.WriteToBatch(wb);
            }

            wb.WriteUInt16((ushort)Consistency);

            if (!protocolVersion.SupportsBatchFlags())
            {
                // if the protocol version doesn't support flags,
                // then it doesn't support the following optional parameters either
                return;
            }
            
            if (protocolVersion.Uses4BytesQueryFlags())
            {
                wb.WriteInt32((int)_batchFlags);
            }
            else
            {
                wb.WriteByte((byte)_batchFlags);
            }

            // this is optional in the protocol but we always set it
            wb.WriteUInt16((ushort)SerialConsistency);

            if (protocolVersion.SupportsTimestamp() && _timestamp != null)
            {
                wb.WriteLong(_timestamp.Value);
            }

            if (protocolVersion.SupportsKeyspaceInRequest() && _keyspace != null)
            {
                wb.WriteString(_keyspace);
            }
        }
    }
}