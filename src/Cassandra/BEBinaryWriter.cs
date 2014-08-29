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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// It represents a protocol writer
    /// </summary>
    internal class BEBinaryWriter
    {
        private readonly ListBackedStream _stream;
        private byte[] _header;
        /// <summary>
        /// protocol version
        /// </summary>
        private byte _version;

        public long Length
        {
            get
            { 
                return _stream.Length; 
            }
        }

        /// <summary>
        /// For testing purposes
        /// </summary>
        internal byte[] GetBuffer()
        {
            var buffer = new byte[_stream.Length];
            _stream.Position = 0;
            _stream.Read(buffer, 0, buffer.Length);
            return buffer;
        }

        public BEBinaryWriter()
        {
            _stream = new ListBackedStream();
            _stream.KeepReferences = true;
        }

        public void WriteByte(byte value)
        {
            this.Write(new [] {value});
        }

        /// <summary>
        /// Writes BE uint 16
        /// </summary>
        public void WriteUInt16(ushort value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            //Invert order
            this.Write(new[] { bytes[1], bytes[0] });
        }

        /// <summary>
        /// Writes BE int 16
        /// </summary>
        public void WriteInt16(short value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            //Invert order
            this.Write(new[] { bytes[1], bytes[0] });
        }

        /// <summary>
        /// Writes BE int
        /// </summary>
        public void WriteInt32(int value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            this.Write(bytes.Reverse().ToArray());
        }

        /// <summary>
        /// Writes Big Endian long
        /// </summary>
        public void WriteLong(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            this.Write(bytes.Reverse().ToArray());
        }

        /// <summary>
        /// Writes protocol <c>string</c> (length + bytes)
        /// </summary>
        public void WriteString(string str)
        {
            var encoding = new UTF8Encoding();
            byte[] bytes = encoding.GetBytes(str);
            WriteUInt16((ushort) bytes.Length);
            this.Write(bytes);
        }

        /// <summary>
        /// Writes protocol <c>long string</c> (length + bytes)
        /// </summary>
        public void WriteLongString(string str)
        {
            var encoding = new UTF8Encoding();
            byte[] bytes = encoding.GetBytes(str);
            WriteInt32(bytes.Length);
            this.Write(bytes);
        }

        /// <summary>
        /// Writes protocol <c>string list</c> (length + bytes)
        /// </summary>
        public void WriteStringList(List<string> l)
        {
            WriteUInt16((ushort) l.Count);
            foreach (string str in l)
            {
                WriteString(str);
            }
        }

        /// <summary>
        /// Writes protocol <c>bytes</c> (length + bytes)
        /// </summary>
        public void WriteBytes(byte[] buffer)
        {
            if (buffer == null)
            {
                WriteInt32(-1);
            }
            else
            {
                WriteInt32(buffer.Length);
                this.Write(buffer);
            }
        }

        /// <summary>
        /// Writes protocol <c>short bytes</c> (length + bytes)
        /// </summary>
        public void WriteShortBytes(byte[] buffer)
        {
            WriteInt16((short) buffer.Length);
            this.Write(buffer);
        }

        public void WriteFrameHeader(byte version, byte flags, short streamId, byte opCode)
        {
            _version = version;
            if (_version < 3)
            {
                if (streamId > 127)
                {
                    throw new ArgumentException("StreamId must be smaller than 128 under protocol version " + version);
                }
                _header = new byte[8]
                {
                    version,
                    flags,
                    (byte) streamId,
                    opCode,
                    //Reserved for the body length
                    0, 0, 0, 0
                };
            }
            else
            {
                var streamIdBytes = BitConverter.GetBytes(streamId).Reverse().ToArray();
                _header = new byte[9]
                {
                    version,
                    flags,
                    streamIdBytes[0],
                    streamIdBytes[1],
                    opCode,
                    //Reserved for the body length
                    0, 0, 0, 0
                };
            }
            this.Write(_header);
        }

        public RequestFrame GetFrame()
        {
            //Save the length in the header
            int bodyLength = (int)_stream.Length - FrameHeader.GetSize(_version);
            byte[] lengthBytes = BitConverter.GetBytes(bodyLength).Reverse().ToArray();
            //The length could start at the 4th or 5th position
            byte lengthOffset = 4;
            if (_version >= 3)
            {
                lengthOffset = 5;
            }
            //Copy the length bytes into the header, by reference will be used in the stream
            Buffer.BlockCopy(lengthBytes, 0, _header, lengthOffset, 4);
            return new RequestFrame(_stream);
        }

        /// <summary>
        /// Writes the complete buffer to the underlying stream
        /// </summary>
        protected void Write(byte[] buffer)
        {
            _stream.Write(buffer, 0, buffer.Length);
        }
    }
}
