namespace Cassandra
{
    internal class WriteTimeoutInfo
    {
        public int BlockFor;
        public ConsistencyLevel ConsistencyLevel;
        public int Received;
        public string WriteType;
    };
}