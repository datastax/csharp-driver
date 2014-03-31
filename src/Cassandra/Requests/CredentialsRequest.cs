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
using System.Collections.Generic;

namespace Cassandra
{
    internal class CredentialsRequest : IRequest
    {
        public const byte OpCode = 0x04;
        private readonly IDictionary<string, string> _credentials;
        private readonly int _streamId;

        public CredentialsRequest(int streamId, IDictionary<string, string> credentials)
        {
            _streamId = streamId;
            _credentials = credentials;
        }

        public RequestFrame GetFrame(byte protocolVersionByte)
        {
            if (protocolVersionByte != RequestFrame.ProtocolV1RequestVersionByte)
                throw new NotSupportedException("Credentials request is supported in C* <= 1.2.x");

            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(protocolVersionByte, 0x00, (byte) _streamId, OpCode);
            wb.WriteUInt16((ushort) _credentials.Count);
            foreach (KeyValuePair<string, string> kv in _credentials)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}