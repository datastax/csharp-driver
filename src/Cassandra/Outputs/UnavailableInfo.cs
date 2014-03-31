namespace Cassandra
{
    internal class UnavailableInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Required;
        public int Alive;
    };
}