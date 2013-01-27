namespace Cassandra
{
    internal class SnappyProtoBufCompressor : IProtoBufComporessor
    {
        public byte[] Decompress(byte[] buffer)
        {
            return Snappy.Snappy.Decompress(buffer, 0, buffer.Length);
        }
    }
}
