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

using System.IO;

namespace Cassandra.Requests
{
    internal class AuthResponseRequest : IRequest
    {
        public const byte OpCode = 0x0F;
        private readonly byte[] _token;

        public int ProtocolVersion { get; set; }

        public AuthResponseRequest(int protocolVersion, byte[] token)
        {
            ProtocolVersion = protocolVersion;
            _token = token;
        }

        public int WriteFrame(short streamId, MemoryStream stream)
        {
            var wb = new FrameWriter(stream);
            wb.WriteFrameHeader((byte)ProtocolVersion, 0x00, streamId, OpCode);
            wb.WriteBytes(_token);
            return wb.Close();
        }
    }
}
