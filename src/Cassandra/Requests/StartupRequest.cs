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

using System.Collections.Generic;
using System.IO;

namespace Cassandra.Requests
{
    internal class StartupRequest : IRequest
    {
        public const byte OpCode = 0x01;
        private readonly IDictionary<string, string> _options;

        public int ProtocolVersion { get; set; }

        public StartupRequest(int protocolVersion, IDictionary<string, string> options)
        {
            ProtocolVersion = protocolVersion;
            _options = options;
        }

        public int WriteFrame(short streamId, MemoryStream stream)
        {
            var wb = new FrameWriter(stream);
            wb.WriteFrameHeader((byte)ProtocolVersion, 0x00, streamId, OpCode);
            wb.WriteUInt16((ushort) _options.Count);
            foreach (var kv in _options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.Close();
        }
    }
}
