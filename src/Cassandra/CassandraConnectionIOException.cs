using System;
using System.IO;

namespace Cassandra
{
    internal class CassandraConnectionIOException : IOException
    {
        public CassandraConnectionIOException(Exception innerException = null)
            : base("Cassandra connection I/O exception", innerException)
        {
        }
    }
}