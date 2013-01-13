namespace Cassandra
{
    internal interface IProtoBufComporessor
    {
        byte[] Decompress(byte[] buffer);
    }
}
