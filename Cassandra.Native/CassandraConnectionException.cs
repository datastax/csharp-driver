using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public class CassandraQueryException : CassandraException
    {
        public List<Exception> InnerInnerExceptions { get; private set; }
        public CassandraQueryException(string message, Exception innerException = null, List<Exception> innerInnerExceptions = null)
            : base(message, innerException)
        {
            InnerInnerExceptions = innerInnerExceptions;
        }
    }

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
