﻿//
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
using System;
using System.IO;
using System.Linq;

namespace Cassandra
{
    internal class FrameHeader
    {
        [Flags]
        internal enum HeaderFlag : byte
        {
            /// <summary>
            /// If set, the frame body is compressed.
            /// </summary>
            Compression =    0x01,
            /// <summary>
            /// For a request frame, this indicate the client requires tracing of the request.
            /// If a response frame has the tracing flag set, its body contains a tracing ID.
            /// </summary>
            Tracing =        0x02,
            /// <summary>
            /// For a request or response frame, this indicates that generic key-value 
            /// custom payload for a custom QueryHandler implementation is present in the frame.
            /// </summary>
            CustomPayload =  0x04,
            /// <summary>
            /// The response contains warnings from the server which 
            /// were generated by the server to go along with this response.
            /// </summary>
            Warning =        0x08
        }

        public const int MaxFrameSize = 256*1024*1024;
        /// <summary>
        /// Protocol version byte (in case of responses 0x81, 0x82, ... in case of requests 0x01, 0x02)
        /// </summary>
        private byte _versionByte;

        /// <summary>
        /// Returns the length of the frame body 
        /// </summary>
        public int BodyLength { get; private set; }

        /// <summary>
        /// Flags applying to this frame..
        /// </summary>
        public HeaderFlag Flags { get; set; }
        
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
            return _versionByte >> 7 == 1 && (_versionByte & 0x07) > 0;
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
        /// Parses the first 8 or 9 bytes and returns a FrameHeader
        /// </summary>
        public static FrameHeader ParseResponseHeader(byte version, byte[] buffer, int offset)
        {
            var header = new FrameHeader()
            {
                _versionByte = buffer[offset++],
                Flags = (HeaderFlag)buffer[offset++]
            };
            if (version < 3)
            {
                //Stream id is a signed byte in v1 and v2 of the protocol
                header.StreamId =  (sbyte)buffer[offset++];
            }
            else
            {
                header.StreamId = BeConverter.ToInt16(buffer, offset);
                offset += 2;
            }
            header.Opcode = buffer[offset++];
            header.BodyLength = BeConverter.ToInt32(Utils.SliceBuffer(buffer, offset, 4));
            return header;
        }

        /// <summary>
        /// Parses the first 8 or 9 bytes from multiple buffers and returns a FrameHeader
        /// </summary>
        public static FrameHeader ParseResponseHeader(byte version, byte[] buffer1, byte[] buffer2)
        {
            var headerBuffer = new byte[version < 3 ? 8 : 9];
            Buffer.BlockCopy(buffer1, 0, headerBuffer, 0, buffer1.Length);
            Buffer.BlockCopy(buffer2, 0, headerBuffer, buffer1.Length, headerBuffer.Length - buffer1.Length);
            return ParseResponseHeader(version, headerBuffer, 0);
        }

        /// <summary>
        /// Gets the protocol version based on the first byte of the header
        /// </summary>
        public static byte GetProtocolVersion(byte[] buffer)
        {
            return (byte)(buffer[0] & 0x7F);
        }
    }
}