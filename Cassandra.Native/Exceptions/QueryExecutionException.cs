using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * Exception related to the execution of a query.
     *
     * This correspond to the exception that Cassandra throw when a (valid) query
     * cannot be executed (TimeoutException, UnavailableException, ...).
     */
    public abstract class QueryExecutionException : CassandraServerException
    {
        public QueryExecutionException(string message)
            : base(message) { }

        public QueryExecutionException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}