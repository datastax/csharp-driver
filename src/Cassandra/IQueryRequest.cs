namespace Cassandra
{
    internal interface IQueryRequest : IRequest
    {
        void WriteToBatch(byte protocolVersion, BEBinaryWriter writer);
    }
}