namespace Cassandra
{
    internal class ReadTimeoutInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public bool IsDataPresent;
    };
}