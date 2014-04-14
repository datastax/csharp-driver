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
    internal class QueryRequest : IQueryRequest
    {
        public const byte OpCode = 0x07;

        private readonly ConsistencyLevel? _consistency;
        private readonly string _cqlQuery;
        private readonly byte _headerFlags;
        private readonly QueryProtocolOptions _queryProtocolOptions;
        private readonly int _streamId;

        public QueryRequest(int streamId, string cqlQuery, bool tracingEnabled, QueryProtocolOptions queryPrtclOptions,
                            ConsistencyLevel? consistency = null)
        {
            _streamId = streamId;
            _cqlQuery = cqlQuery;
            _consistency = consistency;
            _queryProtocolOptions = queryPrtclOptions;
            if (tracingEnabled)
                _headerFlags = 0x02;
        }

        public RequestFrame GetFrame(byte protocolVersionByte)
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(protocolVersionByte, _headerFlags, (byte) _streamId, OpCode);
            wb.WriteLongString(_cqlQuery);

            _queryProtocolOptions.Write(wb, _consistency, protocolVersionByte);

            return wb.GetFrame();
        }

        public void WriteToBatch(BEBinaryWriter wb)
        {
            wb.WriteByte(0); //not a prepared query
            wb.WriteLongString(_cqlQuery);
            if (_queryProtocolOptions.Values == null || _queryProtocolOptions.Values.Length == 0)
                wb.WriteInt16(0); //not values
            else
            {
                wb.WriteUInt16((ushort) _queryProtocolOptions.Values.Length);
                for (int i = 0; i < _queryProtocolOptions.Values.Length; i++)
                {
                    byte[] bytes = TypeInterpreter.InvCqlConvert(_queryProtocolOptions.Values[i]);
                    wb.WriteBytes(bytes);
                }
            }
        }
    }
}
