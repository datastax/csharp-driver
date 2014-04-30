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

using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Cassandra
{
    internal class BEBinaryReader
    {
        private readonly byte[] _buffer = new byte[4];
        private readonly byte[] _longBuffer = new byte[16];
        private readonly IProtoBuf _stream;

        public BEBinaryReader(ResponseFrame input)
        {
            _stream = input.RawStream;
        }

        public byte ReadByte()
        {
            _stream.Read(_buffer, 0, 1);
            return _buffer[0];
        }

        public ushort ReadUInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return (ushort) ((_buffer[0] << 8) | (_buffer[1] & 0xff));
        }

        public short ReadInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return (short) ((_buffer[0] << 8) | (_buffer[1] & 0xff));
        }


        public int ReadInt32()
        {
            _stream.Read(_buffer, 0, 4);
            return (_buffer[0] << 24) | (_buffer[1] << 16 & 0xffffff) | (_buffer[2] << 8 & 0xffff) | (_buffer[3] & 0xff);
        }

        public string ReadString()
        {
            ushort length = ReadUInt16();
            return ReadPureString(length);
        }

        /// <summary>
        /// Reads protocol inet: Ip (4 or 16 bytes) followed by a port (int)
        /// </summary>
        public IPEndPoint ReadInet()
        {
            byte length = ReadByte();
            IPAddress ip;
            if (length == 4)
            {
                _stream.Read(_buffer, 0, length);
                ip = new IPAddress(_buffer);
                return new IPEndPoint(ip, ReadInt32());
            }
            if (length == 16)
            {
                _stream.Read(_longBuffer, 0, length);
                ip = new IPAddress(_longBuffer);
                return new IPEndPoint(ip, ReadInt32());
            }

            throw new DriverInternalError("unknown length of Inet Address");
        }

        public string ReadLongString()
        {
            int length = ReadInt32();
            return ReadPureString(length);
        }

        private string ReadPureString(int length)
        {
            var bytes = new byte[length];
            _stream.Read(bytes, 0, length);
            return Encoding.UTF8.GetString(bytes);
        }

        public List<string> ReadStringList()
        {
            ushort length = ReadUInt16();
            var l = new List<string>();
            for (int i = 0; i < length; i++)
                l.Add(ReadString());
            return l;
        }

        public byte[] ReadBytes()
        {
            int length = ReadInt32();
            if (length < 0) return null;
            var buf = new byte[length];
            Read(buf, 0, length);
            return buf;
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            _stream.Read(buffer, offset, count);
        }

        public void Skip(int count)
        {
            _stream.Skip(count);
        }
    }
}