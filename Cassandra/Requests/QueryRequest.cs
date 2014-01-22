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
﻿namespace Cassandra
{
    internal class QueryRequest : IRequest
    {
        public const byte OpCode = 0x07;

        private readonly int _streamId;
        private readonly string _cqlQuery;
        private readonly ConsistencyLevel _consistency;
        private readonly byte _flags = 0x00;

        public QueryRequest(int streamId, string cqlQuery, ConsistencyLevel consistency, bool tracingEnabled)
        {
            this._streamId = streamId;
            this._cqlQuery = cqlQuery;
            this._consistency = consistency;
            if (tracingEnabled)
                this._flags = 0x02;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(RequestFrame.ProtocolRequestVersionByte, _flags, (byte)_streamId, OpCode);
            wb.WriteLongString(_cqlQuery);
            wb.WriteInt16((short) _consistency);
            return wb.GetFrame();
        }
    }
}
