using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{


    public class CassandraConnectionException : CassandraException
    {
        public CassandraConnectionException(string message, Exception innerException = null)
            : base(message, innerException)
        {
        }
    }

    public class CassandraConncectionClosedUnexpectedlyException : CassandraConnectionException
    {
        public CassandraConncectionClosedUnexpectedlyException()
            : base("cassandra connection closed unexpectedly")
        {
        }
    }

    public class CassandraConncectionIOException : CassandraConnectionException
    {
        public CassandraConncectionIOException(Exception innerException = null)
            : base("cassandra connection io exception", innerException)
        {
        }
    }

    public class CassandraConnectionTimeoutException : CassandraConnectionException
    {
        public CassandraConnectionTimeoutException()
            : base("cassandra connection timeout exception")
        {
        }
    }
}
