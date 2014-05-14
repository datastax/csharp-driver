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
using System.Linq;

namespace Cassandra
{
    internal class FrameHeader
    {
        public const int MaxFrameSize = 256*1024*1024;
        public const int Size = 8;

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

        /// <summary>
        /// Returns the length of the frame body and the header
        /// </summary>
        public int TotalFrameLength
        {
            get
            {
                return this.BodyLength + Size;
            }
        }

        public byte Version { get; set; }

        /// <summary>
        /// Parses the first 8 bytes and returns a FrameHeader
        /// </summary>
        public static FrameHeader Parse(byte[] buffer)
        {
            return new FrameHeader()
            {
                Version = buffer[0],
                Flags = buffer[1],
                StreamId = buffer[2],
                Opcode = buffer[3],
                Len = buffer.Skip(4).Take(4).ToArray()
            };
        }

        public ResponseFrame MakeFrame(IProtoBuf stream)
        {
            int bodyLen = TypeInterpreter.BytesToInt32(Len, 0);

            if (MaxFrameSize - 8 < bodyLen) throw new DriverInternalError("Frame length mismatch");

            var frame = new ResponseFrame {FrameHeader = this, RawStream = stream};
            return frame;
        }
    }
}