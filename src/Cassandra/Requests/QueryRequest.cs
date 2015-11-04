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

namespace Cassandra.Requests
{
    /// <summary>
    /// Represents a protocol QUERY request
    /// </summary>
    internal class QueryRequest : IQueryRequest, ICqlRequest
    {
        public const byte OpCode = 0x07;

        public ConsistencyLevel Consistency
        {
            get { return _queryOptions.Consistency; }
            set { _queryOptions.Consistency = value;}
        }

        public byte[] PagingState
        {
            get { return _queryOptions.PagingState; }
            set { _queryOptions.PagingState = value; }
        }

        public int PageSize
        {
            get { return _queryOptions.PageSize; }
        }

        public ConsistencyLevel SerialConsistency
        {
            get { return _queryOptions.SerialConsistency; }
        }

        public string Query { get { return _cqlQuery; }}

        public IDictionary<string, byte[]> Payload { get; set; }

        public int ProtocolVersion { get; set; }

        private readonly string _cqlQuery;
        private FrameHeader.HeaderFlag _headerFlags;
        private readonly QueryProtocolOptions _queryOptions;

        public QueryRequest(int protocolVersion, string cqlQuery, bool tracingEnabled, QueryProtocolOptions queryOptions)
        {
            //TODO: Replace constructor parameters with IStatement
            ProtocolVersion = protocolVersion;
            _cqlQuery = cqlQuery;
            _queryOptions = queryOptions;
            if (tracingEnabled)
            {
                _headerFlags = FrameHeader.HeaderFlag.Tracing;
            }
            if (queryOptions == null)
            {
                throw new ArgumentNullException("queryOptions");
            }
            if (Consistency.IsSerialConsistencyLevel())
            {
                throw new RequestInvalidException("Serial consistency specified as a non-serial one.");
            }
            if (queryOptions.SerialConsistency != ConsistencyLevel.Any && queryOptions.SerialConsistency.IsSerialConsistencyLevel() == false)
            {
                throw new RequestInvalidException("Non-serial consistency specified as a serial one.");
            }
            if (protocolVersion < 3)
            {
                //Features supported in protocol v3 and above
                if (queryOptions.Timestamp != null)
                {
                    throw new NotSupportedException("Timestamp for query is supported in Cassandra 2.1 and above.");
                }
                if (queryOptions.ValueNames != null && queryOptions.ValueNames.Count > 0)
                {
                    throw new NotSupportedException("Query parameter names feature is supported in Cassandra 2.1 and above.");
                }
            }
        }

        public int WriteFrame(short streamId, MemoryStream stream)
        {
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
            wb.WriteLongString(_cqlQuery);
            _queryOptions.Write(wb, (byte)ProtocolVersion, false);
            return wb.Close();
        }

        public void WriteToBatch(byte protocolVersion, FrameWriter wb)
        {
            //not a prepared query
            wb.WriteByte(0);
            wb.WriteLongString(_cqlQuery);
            if (_queryOptions.Values == null || _queryOptions.Values.Length == 0)
            {
                //not values
                wb.WriteInt16(0);
            }
            else
            {
                wb.WriteUInt16((ushort) _queryOptions.Values.Length);
                for (var i = 0; i < _queryOptions.Values.Length; i++)
                {
                    var bytes = TypeCodec.Encode(protocolVersion, _queryOptions.Values[i]);
                    wb.WriteBytes(bytes);
                }
            }
        }
    }
}
