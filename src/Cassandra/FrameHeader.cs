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
        /// Protocol version byte (in case of responses 0x81, 0x82, ... in case of requests 0x01, 0x02)
        /// </summary>
        private byte _versionByte;

        /// <summary>
        /// Returns the length of the frame body 
        /// </summary>
        public int BodyLength
        {
            get
            {
                return TypeCodec.BytesToInt32(Len, 0);
            }
        }
        
        public byte Flags { get; set; }
        
        public byte[] Len = new byte[4];
        
        public byte Opcode { get; set; }
        
        public short StreamId { get; set; }

        /// <summary>
        /// Protocol version of the protocol (1, 2, 3)
        /// </summary>
        public byte Version
        {
            get
            {
                return (byte)(_versionByte & 0x07);
            }
        }

        /// <summary>
        /// Determines if the response is valid by checking the version byte
        /// </summary>
        public bool IsValidResponse()
        {
            return this._versionByte >> 7 == 1 && (_versionByte & 0x07) > 0;
        }

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
        public static FrameHeader ParseResponseHeader(byte version, byte[] buffer, int offset)
        {
            var header = new FrameHeader()
            {
                _versionByte = buffer[offset++],
                Flags = buffer[offset++]
            };
            if (version < 3)
            {
                //Stream id is a signed byte in v1 and v2 of the protocol
                header.StreamId =  (sbyte)buffer[offset++];
            }
            else
            {
                header.StreamId = BitConverter.ToInt16(new byte[] { buffer[offset + 1], buffer[offset] }, 0);
                offset += 2;
            }
            header.Opcode = buffer[offset++];
            header.Len = buffer.Skip(offset).Take(4).ToArray();
            return header;
        }
    }
}