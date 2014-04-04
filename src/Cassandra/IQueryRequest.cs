namespace Cassandra
{
    internal interface IQueryRequest : IRequest
    {
        void WriteToBatch(BEBinaryWriter writer);
    }
}