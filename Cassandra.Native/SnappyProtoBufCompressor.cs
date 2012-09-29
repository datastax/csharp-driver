using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class SnappyProtoBufCompressor : IProtoBufComporessor
    {
        public byte[] Decompress(byte[] buffer)
        {
            return Snappy.Snappy.Decompress(buffer, 0, buffer.Length);
        }
    }
}
