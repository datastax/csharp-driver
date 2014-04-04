using System;
using System.IO;

namespace Cassandra
{
    internal class CassandraConnectionIOException : IOException
    {
        public CassandraConnectionIOException(Exception innerException = null)
            : base("cassandra connection io exception", innerException)
        {
        }
    }
}