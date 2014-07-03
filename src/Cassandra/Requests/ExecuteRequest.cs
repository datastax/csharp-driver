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

namespace Cassandra
{
    /// <summary>
    /// Represents a protocol EXECUTE request
    /// </summary>
    internal class ExecuteRequest : IQueryRequest, ICqlRequest
    {
        public const byte OpCode = 0x0A;

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

        private readonly byte _flags;
        private readonly byte[] _id;
        private readonly RowSetMetadata _metadata;
        private readonly QueryProtocolOptions _queryProtocolOptions;

        public ExecuteRequest(int protocolVersion, byte[] id, RowSetMetadata metadata, bool tracingEnabled, QueryProtocolOptions queryProtocolOptions)
        {
            ProtocolVersion = protocolVersion;
            if (metadata != null && queryProtocolOptions.Values.Length != metadata.Columns.Length)
            {
                throw new ArgumentException("Number of values does not match with number of prepared statement markers(?).", "values");
            }
            _id = id;
            _metadata = metadata;
            _queryProtocolOptions = queryProtocolOptions;
            if (tracingEnabled)
            {
                _flags = 0x02;
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
            wb.WriteFrameHeader((byte)ProtocolVersion, _flags, streamId, OpCode);
            wb.WriteShortBytes(_id);
            _queryProtocolOptions.Write(wb, (byte)ProtocolVersion);

            return wb.GetFrame();
        }

        public void WriteToBatch(byte protocolVersion, BEBinaryWriter wb)
        {
            wb.WriteByte(1); //prepared query
            wb.WriteShortBytes(_id);
            wb.WriteUInt16((ushort) _queryProtocolOptions.Values.Length);
            for (int i = 0; i < _metadata.Columns.Length; i++)
            {
                byte[] bytes = TypeCodec.Encode(protocolVersion, _queryProtocolOptions.Values[i]);
                wb.WriteBytes(bytes);
            }
        }
    }
}