//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  A Cassandra timeout during a query. Such an exception is returned when the
    ///  query has been tried by Cassandra but cannot be achieved with the requested
    ///  consistency level within the rpc timeout set for Cassandra.
    /// </summary>
    public abstract class QueryTimeoutException : QueryExecutionException
    {
        /// <summary>
        ///  Gets the consistency level of the operation that time outed. 
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the number of replica that had acknowledged/responded to the operation before it time outed. 
        /// </summary>
        public int ReceivedAcknowledgements { get; private set; }

        /// <summary>
        ///  Gets the minimum number of replica acknowledgements/responses that were required to fulfill the operation. 
        /// </summary>
        public int RequiredAcknowledgements { get; private set; }

        public QueryTimeoutException(string message, ConsistencyLevel consistencyLevel, int received, int required)
            : base(message)
        {
            ConsistencyLevel = consistencyLevel;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
        }
    }
}
