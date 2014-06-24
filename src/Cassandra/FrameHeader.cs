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
using System.IO;
using System.Linq;

namespace Cassandra
{
    internal class FrameHeader
    {
        public const int MaxFrameSize = 256*1024*1024;

        /// <summary>
        /// Returns the length of the frame body 
        /// </summary>
        public int BodyLength
        {
            get
            {
                return TypeInterpreter.BytesToInt32(Len, 0);
            }
        }
        
        public byte Flags { get; set; }
        
        public byte[] Len = new byte[4];
        
        public byte Opcode { get; set; }
        
        public byte StreamId { get; set; }

        public byte Version { get; set; }

        /// <summary>
        /// Gets the size of the protocol header, depending on the version of the protocol
        /// </summary>
        /// <param name="version">Version of the protocol used</param>
        public static byte GetSize(byte version)
        {
            if (version >= 3)
            {
                return 9;
            }
            return 8;
        }

        /// <summary>
        /// Parses the first 8 bytes and returns a FrameHeader
        /// </summary>
        public static FrameHeader ParseResponseHeader(byte[] buffer, int offset)
        {
            return new FrameHeader()
            {
                Version = buffer[offset + 0],
                Flags = buffer[offset + 1],
                StreamId = buffer[offset + 2],
                Opcode = buffer[offset + 3],
                Len = buffer.Skip(offset + 4).Take(4).ToArray()
            };
        }
    }
}