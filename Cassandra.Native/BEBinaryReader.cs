using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Net;

namespace Cassandra.Native
{
    internal class BEBinaryReader
    {
        IProtoBuf _stream;
        byte[] _buffer = new byte[4];
        byte[] _longBuffer = new byte[16];

        public BEBinaryReader(ResponseFrame input) { _stream = input.RawStream; }

        public byte ReadByte()
        {
            _stream.Read(_buffer, 0, 1);
            return (byte)_buffer[0];
        }

        public ushort ReadUInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return (ushort)((_buffer[0] << 8) | (_buffer[1] & 0xff));
        }

        public short ReadInt16()
        {
            _stream.Read(_buffer, 0, 2);
            return (short)((_buffer[0] << 8) | (_buffer[1] & 0xff)); 
        }


        public int ReadInt32()
        {
            _stream.Read(_buffer, 0, 4);
            return (int)((_buffer[0] << 24) | (_buffer[1] << 16 & 0xffffff) | (_buffer[2] << 8 & 0xffff) | (_buffer[3] & 0xff));
        }

        public string ReadString()
        {
            var length = ReadUInt16();
            return readPureString(length);
        }

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
            else if (length == 16)
            {
                _stream.Read(_longBuffer, 0, length);
                ip = new IPAddress(_longBuffer);
                return new IPEndPoint(ip, ReadInt32());
            }

            throw new CassandraClientProtocolViolationException("unknown length of Inet Address");
        }

        public string ReadLongString()
        {
            int length = ReadInt32();
            return readPureString(length);
        }

        string readPureString(int length)
        {
            var bytes = new byte[length];
            _stream.Read(bytes, 0, length);
            return System.Text.Encoding.UTF8.GetString(bytes);
        }

        public List<string> ReadStringList()
        {
            var length = ReadUInt16();
            List<string> l = new List<string>();
            for (int i = 0; i < length; i++)
                l.Add(ReadString());
            return l;
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
