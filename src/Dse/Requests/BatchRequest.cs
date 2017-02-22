//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
        public const byte OpCode = 0x0D;

        private FrameHeader.HeaderFlag _headerFlags;
        private readonly QueryProtocolOptions.QueryFlags _batchFlags = 0;
        private readonly ICollection<IQueryRequest> _requests;
        private readonly BatchType _type;
        private readonly DateTimeOffset? _timestamp;
        private readonly ConsistencyLevel? _serialConsistency;

        public ConsistencyLevel Consistency { get; set; }

        public IDictionary<string, byte[]> Payload { get; set; }

        public BatchRequest(ProtocolVersion protocolVersion, BatchStatement statement, ConsistencyLevel consistency)
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
            _timestamp = statement.Timestamp;
            if (statement.IsTracing)
            {
                _headerFlags = FrameHeader.HeaderFlag.Tracing;
            }
            if (statement.SerialConsistencyLevel != ConsistencyLevel.Any)
            {
                if (!protocolVersion.SupportsTimestamp())
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
                if (!protocolVersion.SupportsTimestamp())
                {
                    throw new NotSupportedException("Timestamp for BATCH request is supported in Cassandra 2.1 or above.");
                }
                _batchFlags |= QueryProtocolOptions.QueryFlags.WithDefaultTimestamp;
            }
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
            wb.WriteInt16((short) _requests.Count);
            foreach (var br in _requests)
            {
                br.WriteToBatch(wb);
            }
            wb.WriteInt16((short) Consistency);
            if (protocolVersion.SupportsTimestamp())
            {
                wb.WriteByte((byte)_batchFlags);
                if (_serialConsistency != null)
                {
                    wb.WriteInt16((short)_serialConsistency.Value);
                }
                if (_timestamp != null)
                {
                    //Expressed in microseconds
                    wb.WriteLong(TypeSerializer.SinceUnixEpoch(_timestamp.Value).Ticks / 10);
                }
            }
            return wb.Close();
        }
    }
}
