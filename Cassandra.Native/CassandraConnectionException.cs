using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra
{
    internal class CassandraConncectionIOException : IOException
    {
        public CassandraConncectionIOException(Exception innerException = null)
            : base("cassandra connection io exception", innerException)
        {
        }
    }

    internal class CassandraConnectionTimeoutException : TimeoutException
    {
        public CassandraConnectionTimeoutException()
            : base("cassandra connection timeout exception")
        {
        }
    }
}
