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
using System.Text;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// It represents a big endian protocol-aware writer
    /// </summary>
    internal class FrameWriter
    {
        private readonly MemoryStream _stream;
        private readonly ISerializer _serializer;
        private readonly long _offset;
        private readonly ProtocolVersion _version;

        public long Length
        {
            get { return _stream.Length; }
        }

        internal ISerializer Serializer
        {
            get { return _serializer; }
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

        public FrameWriter(MemoryStream stream, ISerializer serializer)
        {
            _stream = stream;
            _serializer = serializer;
            _offset = stream.Position;
            _version = serializer.ProtocolVersion;
        }

        public void WriteByte(byte value)
        {
            _stream.WriteByte(value);
        }

        /// <summary>
        /// Writes BE uint 16
        /// </summary>
        public void WriteUInt16(ushort value)
        {
            Write(BeConverter.GetBytes(value));
        }

        /// <summary>
        /// Writes BE int
        /// </summary>
        public void WriteInt32(int value)
        {
            Write(BeConverter.GetBytes(value));
        }

        /// <summary>
        /// Writes Big Endian long
        /// </summary>
        public void WriteLong(long value)
        {
            Write(BeConverter.GetBytes(value));
        }

        /// <summary>
        /// Writes protocol <c>string</c> (length + bytes)
        /// </summary>
        public void WriteString(string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);
            WriteUInt16((ushort) bytes.Length);
            Write(bytes);
        }

        /// <summary>
        /// Writes protocol <c>long string</c> (length + bytes)
        /// </summary>
        public void WriteLongString(string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);
            WriteInt32(bytes.Length);
            Write(bytes);
        }

        /// <summary>
        /// Writes protocol <c>string list</c> (length + bytes)
        /// </summary>
        public void WriteStringList(ICollection<string> l)
        {
            WriteUInt16((ushort) l.Count);
            foreach (var str in l)
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
                return;
            }
            if (buffer == Serialization.SerializerManager.UnsetBuffer)
            {
                WriteInt32(-2);
                return;
            }
            WriteInt32(buffer.Length);
            Write(buffer);
        }

        /// <summary>
        /// Serializes and writes as protocol <c>bytes</c> (length + bytes)
        /// </summary>
        public void WriteAsBytes(object value)
        {
            WriteBytes(_serializer.Serialize(value));
        }

        /// <summary>
        /// Writes protocol <c>short bytes</c> (length + bytes)
        /// </summary>
        public void WriteShortBytes(byte[] buffer)
        {
            WriteUInt16((ushort) buffer.Length);
            Write(buffer);
        }

        /// <summary>
        /// Writes a protocol bytes maps
        /// </summary>
        public void WriteBytesMap(IDictionary<string, byte[]> map)
        {
            WriteUInt16((ushort)map.Count);
            foreach (var kv in map)
            {
                WriteString(kv.Key);
                WriteBytes(kv.Value);
            }
        }

        /// <summary>
        /// Writes the frame header, leaving body length to 0
        /// </summary>
        public void WriteFrameHeader(byte flags, short streamId, byte opCode)
        {
            byte[] header;
            if (!_version.Uses2BytesStreamIds())
            {
                //8 bytes for the header, dedicating 1 for the streamId
                if (streamId > 127)
                {
                    throw new ArgumentException("StreamId must be smaller than 128 under protocol version " + _version);
                }
                header = new byte[]
                {
                    (byte) _version,
                    flags,
                    (byte) streamId,
                    opCode,
                    //Reserved for the body length
                    0, 0, 0, 0
                };
                Write(header);
                return;
            }
            //9 bytes for the header, dedicating 2 for the streamId
            var streamIdBytes = BeConverter.GetBytes(streamId);
            header = new byte[]
            {
                (byte) _version,
                flags,
                streamIdBytes[0],
                streamIdBytes[1],
                opCode,
                //Reserved for the body length
                0, 0, 0, 0
            };
            Write(header);
        }

        /// <summary>
        /// Writes the complete buffer to the underlying stream
        /// </summary>
        protected void Write(byte[] buffer)
        {
            _stream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes the body length in the frame and returns the frame length
        /// </summary>
        public int Close()
        {
            //Set the length in the header
            //MemoryStream implementation length and offset are ints, so cast is safe
            var frameLength = Convert.ToInt32(_stream.Length - _offset);
            var lengthBytes = BeConverter.GetBytes(frameLength - _version.GetHeaderSize());
            //The length could start at the 4th or 5th position
            long lengthOffset = 4;
            if (_version.Uses2BytesStreamIds())
            {
                lengthOffset = 5;
            }
            //Set the position of the stream where the frame body length should be written
            _stream.Position = _offset + lengthOffset;
            _stream.Write(lengthBytes, 0, lengthBytes.Length);
            _stream.Position = _stream.Length;
            return frameLength;
        }
    }
}
