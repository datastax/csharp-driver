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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Cassandra
{
    internal class BEBinaryWriter
    {
        private readonly BinaryWriter _base;
        private int _frameSizePos = -1;

        public long Position
        {
            get { return _base.BaseStream.Position; }
        }

        public long Length
        {
            get { return _base.BaseStream.Length; }
        }

        public BEBinaryWriter()
        {
            _base = new BinaryWriter(new MemoryTributary());
        }

        public byte[] GetBuffer()
        {
            return (_base.BaseStream as MemoryTributary).ToArray();
        }

        public void WriteByte(byte value)
        {
            _base.Write(value);
        }

        public void WriteUInt16(ushort value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            Debug.Assert(bytes.Length == 2);

            _base.Write(bytes[1]);
            _base.Write(bytes[0]);
        }

        public void WriteInt16(short value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            Debug.Assert(bytes.Length == 2);

            _base.Write(bytes[1]);
            _base.Write(bytes[0]);
        }

        public void WriteInt32(int value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            Debug.Assert(bytes.Length == 4);

            _base.Write(bytes[3]);
            _base.Write(bytes[2]);
            _base.Write(bytes[1]);
            _base.Write(bytes[0]);
        }

        public void WriteString(string str)
        {
            var encoding = new UTF8Encoding();
            byte[] bytes = encoding.GetBytes(str);
            WriteUInt16((ushort) bytes.Length);
            _base.Write(bytes);
        }

        public void WriteLongString(string str)
        {
            var encoding = new UTF8Encoding();
            byte[] bytes = encoding.GetBytes(str);
            WriteInt32(bytes.Length);
            _base.Write(bytes);
        }

        public void WriteStringList(List<string> l)
        {
            WriteUInt16((ushort) l.Count);
            foreach (string str in l)
                WriteString(str);
        }

        public void WriteBytes(byte[] buffer)
        {
            if (buffer == null)
            {
                WriteInt32(-1);
            }
            else
            {
                WriteInt32(buffer.Length);
                _base.Write(buffer);
            }
        }

        public void WriteShortBytes(byte[] buffer)
        {
            WriteInt16((short) buffer.Length);
            _base.Write(buffer);
        }

        public void WriteFrameSize()
        {
            _frameSizePos = (int) _base.Seek(0, SeekOrigin.Current);

            _base.BaseStream.Seek(4, SeekOrigin.Current); //Reserving space for "length of the frame body" value
            _base.BaseStream.SetLength(_base.BaseStream.Length + 4);
        }

        public void WriteFrameHeader(byte version, byte flags, byte streamId, byte opCode)
        {
            WriteByte(version);
            WriteByte(flags);
            WriteByte(streamId);
            WriteByte(opCode);
            WriteFrameSize();
        }


        public RequestFrame GetFrame()
        {
            var len = (int) _base.Seek(0, SeekOrigin.Current);
            Debug.Assert(_frameSizePos != -1);
            _base.Seek(_frameSizePos, SeekOrigin.Begin);
            WriteInt32(len - 8);
            return new RequestFrame {Buffer = (MemoryTributary) _base.BaseStream};
        }
    }
}