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
using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class CredentialsRequest : IRequest
    {
        public const byte OpCode = 0x04;
        private readonly IDictionary<string, string> _credentials;

        public CredentialsRequest(IDictionary<string, string> credentials)
        {
            _credentials = credentials;
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            if (serializer.ProtocolVersion != ProtocolVersion.V1)
            {
                throw new NotSupportedException("Credentials request is only supported in C* = 1.2.x");
            }

            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            wb.WriteUInt16((ushort) _credentials.Count);
            foreach (var kv in _credentials)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.Close();
        }
    }
}
