//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  A Cassandra failure (non-timeout) during a write query.
    /// </summary>
    public class WriteFailureException : QueryExecutionException
    {
        /// <summary>
        /// Gets the type of the write (SIMPLE / BATCH / ...)
        /// </summary>
        public string WriteType { get; private set; }
        /// <summary>
        ///  Gets the consistency level of the operation
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the number of replica that had acknowledged/responded to the operation
        /// </summary>
        public int ReceivedAcknowledgements { get; private set; }

        /// <summary>
        ///  Gets the minimum number of replica acknowledgements/responses that were required to fulfill the operation. 
        /// </summary>
        public int RequiredAcknowledgements { get; private set; }

        /// <summary>
        /// Gets the number of nodes that experienced a failure while executing the request.
        /// </summary>
        public int Failures { get; private set; }

        public WriteFailureException(ConsistencyLevel consistency, int received, int required, string writeType, int failures) :
                                         base(string.Format(
                                             "Cassandra timeout during write query at consistency {0} ({1} replica(s) acknowledged the write over {2} required)",
                                             consistency.ToString().ToUpper(), received, required))
        {
            ConsistencyLevel = consistency;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
            WriteType = writeType;
            Failures = failures;
        }
    }
}
