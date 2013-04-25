using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;

namespace Cassandra
{
    internal partial class BEBinaryWriter
    {
        readonly BinaryWriter _base;
        public BEBinaryWriter() { _base = new BinaryWriter(new MemoryStream()); }

        public long Position { get { return _base.BaseStream.Position; } }

        public long Length { get { return _base.BaseStream.Length; } }

        public byte[] GetBuffer()
        {
            return (_base.BaseStream as MemoryStream).GetBuffer();
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
            var encoding = new System.Text.UTF8Encoding();
            var bytes = encoding.GetBytes(str);
            WriteUInt16((ushort)bytes.Length);
            _base.Write(bytes);            
        }

        public void WriteLongString(string str)
        {
            var encoding = new System.Text.UTF8Encoding();
            var bytes = encoding.GetBytes(str);
            WriteInt32(bytes.Length);
            _base.Write(bytes);
        }

        public void WriteStringList(List<string> l)
        {
            WriteUInt16((ushort)l.Count);
            foreach (var str in l)
                WriteString(str);
        }

        public void WriteBytes(byte[] buffer)
        {
            WriteInt32(buffer.Length);
            _base.Write(buffer);
        }

        public void WriteShortBytes(byte[] buffer)
        {
            WriteInt16((short)buffer.Length);
            _base.Write(buffer);
        }
    }
}
