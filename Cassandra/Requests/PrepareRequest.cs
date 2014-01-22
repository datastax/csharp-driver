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
    internal class PrepareRequest : IRequest
    {
        public const byte OpCode = 0x09;

        readonly int _streamId;
        readonly string _cqlQuery;
        public PrepareRequest(int streamId, string cqlQuery)
        {
            this._streamId = streamId;
            this._cqlQuery = cqlQuery;
        }
        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(RequestFrame.ProtocolRequestVersionByte, 0x00, (byte)_streamId, OpCode);
            wb.WriteLongString(_cqlQuery);
            return wb.GetFrame();
        }
    }
}
