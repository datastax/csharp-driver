namespace Cassandra
{
    internal class WriteTimeoutInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public string WriteType;
    };
}