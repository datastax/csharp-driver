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

using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Represents a protocol-aware forward reader 
    /// </summary>
    internal class FrameReader
    {
        /// <summary>
        /// Reusable buffer for reading 2-4 byte types
        /// </summary>
        private readonly byte[] _buffer = new byte[4];
        private readonly Stream _stream;
        private readonly ISerializer _serializer;

        internal ISerializer Serializer
        {
            get { return _serializer; }
        }

        public FrameReader(Stream stream, ISerializer serializer)
        {
            _stream = stream;
            _serializer = serializer;
        }

        public byte ReadByte()
        {
            _stream.Read(_buffer, 0, 1);
            return _buffer[0];
        }

        /// <summary>
        /// Parses a ushort from the following 2 bytes
        /// </summary>
        public ushort ReadUInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return BeConverter.ToUInt16(_buffer);
        }

        /// <summary>
        /// Parses a ushort from the following 2 bytes
        /// </summary>
        public short ReadInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return BeConverter.ToInt16(_buffer);
        }

        public int ReadInt32()
        {
            _stream.Read(_buffer, 0, 4);
            return BeConverter.ToInt32(_buffer);
        }

        public string ReadString()
        {
            var length = ReadInt16();
            return ReadStringByLength(length);
        }

        public string ReadLongString()
        {
            var length = ReadInt32();
            return ReadStringByLength(length);
        }

        private string ReadStringByLength(int length)
        {
            var bytes = new byte[length];
            _stream.Read(bytes, 0, length);
            return Encoding.UTF8.GetString(bytes);
        }

        /// <summary>
        /// Reads a protocol string list
        /// </summary>
        public string[] ReadStringList()
        {
            var length = ReadInt16();
            if (length <= 0)
            {
                return new string[0];
            }
            var arr = new string[length];
            for (var i = 0; i < length; i++)
            {
                arr[i] = ReadString();
            }
            return arr;
        }

        /// <summary>
        /// Reads protocol inet: Ip (4 or 16 bytes) followed by a port (int)
        /// </summary>
        public IPEndPoint ReadInet()
        {
            var length = ReadByte();
            IPAddress ip;
            if (length == 4)
            {
                _stream.Read(_buffer, 0, length);
                ip = new IPAddress(_buffer);
                return new IPEndPoint(ip, ReadInt32());
            }
            if (length == 16)
            {
                var buffer = new byte[16];
                _stream.Read(buffer, 0, length);
                ip = new IPAddress(buffer);
                return new IPEndPoint(ip, ReadInt32());
            }
            throw new DriverInternalError("Unknown length of Inet Address");
        }

        /// <summary>
        /// Reads a protocol bytes map
        /// </summary>
        public Dictionary<string, byte[]> ReadBytesMap()
        {
            //A [short] n, followed by n pair <k><v> where <k> is a
            //[string] and <v> is a [bytes].
            var length = ReadInt16();
            if (length < 0)
            {
                return null;
            }
            var map = new Dictionary<string, byte[]>();
            for (var i = 0; i < length; i++) 
            {
                map[ReadString()] = ReadBytes();
            }
            return map;
        }

        /// <summary>
        /// Reads the protocol bytes, retrieving the int length and reading the subsequent amount of bytes 
        /// </summary>
        public byte[] ReadBytes()
        {
            var length = ReadInt32();
            if (length < 0)
            {
                return null;
            }
            var buf = new byte[length];
            Read(buf, 0, length);
            return buf;
        }

        /// <summary>
        /// Reads protocol [short bytes].
        /// </summary>
        public byte[] ReadShortBytes()
        {
            var length = ReadInt16();
            var buffer = new byte[length];
            Read(buffer, 0, length);
            return buffer;
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            _stream.Read(buffer, offset, count);
        }

        /// <summary>
        /// Reads from the internal stream, starting from offset, the amount of bytes defined by count and deserializes
        /// the bytes.
        /// </summary>
        internal object ReadFromBytes(byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            _stream.Read(buffer, offset, length);
            return _serializer.Deserialize(buffer, 0, length, typeCode, typeInfo);
        }
    }
}
