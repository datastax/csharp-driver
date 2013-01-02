using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * A Cassandra timeout during a query.
     *
     * Such an exception is returned when the query has been tried by Cassandra but
     * cannot be achieved with the requested consistency level within the rpc
     * timeout set for Cassandra.
     */
    public abstract class QueryTimeoutException : QueryExecutionException
    {
        public ConsistencyLevel ConsistencyLevel { get; private set; }
        public int Received { get; private set; }
        public int BlockFor { get; private set; }

        public QueryTimeoutException(string message, ConsistencyLevel ConsistencyLevel, int Received, int BlockFor)
            : base(message) { this.ConsistencyLevel = ConsistencyLevel; this.Received = Received; this.BlockFor = BlockFor; }

    }
}