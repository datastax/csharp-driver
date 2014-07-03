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

namespace Cassandra
{
    /// <summary>
    /// Represents a protocol QUERY request
    /// </summary>
    internal class QueryRequest : IQueryRequest, ICqlRequest
    {
        public const byte OpCode = 0x07;

        public ConsistencyLevel Consistency
        {
            get
            {
                return _queryProtocolOptions.Consistency;
            }
            set
            {
                _queryProtocolOptions.Consistency = value;
            }
        }

        public int ProtocolVersion { get; set; }

        private readonly string _cqlQuery;
        private readonly byte _headerFlags;
        private readonly QueryProtocolOptions _queryProtocolOptions;

        public QueryRequest(int protocolVersion, string cqlQuery, bool tracingEnabled, QueryProtocolOptions queryPrtclOptions)
        {
            ProtocolVersion = protocolVersion;
            _cqlQuery = cqlQuery;
            _queryProtocolOptions = queryPrtclOptions;
            if (tracingEnabled)
            {
                _headerFlags = 0x02;
            }

            if (this.Consistency >= ConsistencyLevel.Serial)
            {
                throw new RequestInvalidException("Serial consistency specified as a non-serial one.");
            }
            if (_queryProtocolOptions.Flags.HasFlag(QueryProtocolOptions.QueryFlags.WithSerialConsistency))
            {
                if (_queryProtocolOptions.SerialConsistency < ConsistencyLevel.Serial)
                {
                    throw new RequestInvalidException("Non-serial consistency specified as a serial one.");
                }
            }
        }

        public RequestFrame GetFrame(short streamId)
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader((byte)ProtocolVersion, _headerFlags, streamId, OpCode);
            wb.WriteLongString(_cqlQuery);

            _queryProtocolOptions.Write(wb, (byte)ProtocolVersion);

            return wb.GetFrame();
        }

        public void WriteToBatch(byte protocolVersion, BEBinaryWriter wb)
        {
            wb.WriteByte(0); //not a prepared query
            wb.WriteLongString(_cqlQuery);
            if (_queryProtocolOptions.Values == null || _queryProtocolOptions.Values.Length == 0)
            {
                wb.WriteInt16(0); //not values
            }
            else
            {
                wb.WriteUInt16((ushort) _queryProtocolOptions.Values.Length);
                for (int i = 0; i < _queryProtocolOptions.Values.Length; i++)
                {
                    byte[] bytes = TypeCodec.Encode(protocolVersion, _queryProtocolOptions.Values[i]);
                    wb.WriteBytes(bytes);
                }
            }
        }
    }
}
