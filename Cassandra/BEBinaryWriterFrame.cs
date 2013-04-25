using System;
using System.IO;
using System.Diagnostics;

namespace Cassandra
{
    internal partial class BEBinaryWriter
    {
        private int _frameSizePos = -1;

        public void WriteFrameSize()
        {
            _frameSizePos = (int)_base.Seek(0, SeekOrigin.Current);
            WriteInt32(0);
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
            var len = (int)_base.Seek(0, SeekOrigin.Current);
            Debug.Assert(_frameSizePos != -1);
            _base.Seek(_frameSizePos, SeekOrigin.Begin);
            WriteInt32(len - 8);
            var buffer = new byte[len];
            Buffer.BlockCopy((_base.BaseStream as MemoryStream).GetBuffer(), 0, buffer, 0, len);
            return new RequestFrame() { Buffer = buffer };
        }
    }
}
