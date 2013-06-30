namespace Cassandra
{
    internal class SnappyProtoBufCompressor : IProtoBufComporessor
    {
        public byte[] Decompress(byte[] buffer)
        {
            return Snappy.SnappyDecompressor.Uncompress(buffer,0,buffer.Length);
        }
    }
}
