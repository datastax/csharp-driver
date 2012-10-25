using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public class CassandraException : Exception
    {
        public CassandraException(string message)
            : base(message) { }

        public CassandraException(string message, Exception innerException)
            : base(message, innerException) { }
    }

    public class CassandraClientException : CassandraException
    {
        public CassandraClientException(string message)
            : base(message) { }

        public CassandraClientException(string message, Exception innerException)
            : base(message, innerException) { }
    }

    public class CassandraClientConfigurationException : CassandraClientException
    {
        public CassandraClientConfigurationException(string message)
            : base(message) { }

        public CassandraClientConfigurationException(string message, Exception innerException)
            : base(message, innerException) { }
    }

    public class CassandraClientProtocolViolationException : CassandraClientException
    {
        public CassandraClientProtocolViolationException(string message)
            : base(message) { }

        public CassandraClientProtocolViolationException(string message, Exception innerException)
            : base(message, innerException) { }
    }

    public class CassandraClientAsyncOperationException : CassandraClientException
    {
        public CassandraClientAsyncOperationException(Exception innerException)
            : base("Async Operation Failed", innerException) { }
    }
}
