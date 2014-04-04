namespace Cassandra
{
    internal interface IProtoBuf
    {
        void Write(byte[] buffer, int offset, int count);
        void WriteByte(byte b);
        void Read(byte[] buffer, int offset, int count);
        void Skip(int count);
    }
}