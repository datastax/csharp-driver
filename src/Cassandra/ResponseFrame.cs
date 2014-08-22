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
namespace Cassandra
{
    internal class ResponseFrame
    {
        public const byte ProtocolV1ResponseVersionByte = 0x81;
        public const byte ProtocolV2ResponseVersionByte = 0x82;

        /// <summary>
        /// The 8 byte protocol header
        /// </summary>
        public FrameHeader Header { get; set; }

        /// <summary>
        /// A stream representing the frame body
        /// </summary>
        public Stream Body { get; set; }

        public ResponseFrame(FrameHeader header, Stream body)
        {
            Header = header;
            Body = body;
        }
    }
}