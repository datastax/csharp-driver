namespace Cassandra
{
    internal class ReadTimeoutInfo
    {
        public int BlockFor;
        public ConsistencyLevel ConsistencyLevel;
        public bool IsDataPresent;
        public int Received;
    };
}