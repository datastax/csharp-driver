using System;

namespace Cassandra
{
    internal class CassandraConnectionTimeoutException : TimeoutException
    {
        public CassandraConnectionTimeoutException()
            : base("cassandra connection timeout exception")
        {
        }
    }
}